import requests
import netrc
import geopandas as gpd
from datetime import datetime, timedelta
import sys
import os
from concurrent.futures import ThreadPoolExecutor
from email.message import Message
import threading

class CDSE:
    __credentials = None
    __token_endpoint = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
    __access_token = None
    __access_token_expires = None
    __refresh_token = None
    __refresh_token_expires = None

    collection = None
    processing_level = None

    def __init__(self, credentials = None):
        '''
            credentials(optional): (username, password) tuple used for acquiring an access token from the identity server. 
            When unspecified, .netrc is used
        '''
        self.__credentials = credentials
        if self.__credentials == None:
            self.__credentials = self.__get_netrc_credentials()
        if self.__credentials == None:
            raise Exception("No credentials provided, and no credentials found in ~/.netrc")
        
    def set_collection(self, collection):
        self.collection = collection
    
    def set_processing_level(self, processing_level):
        self.processing_level = processing_level
    
    def query(self, start_date = None, end_date = None, footprint = None, cloudcover = 0, max_records = None):
        self.__validate_required_params_present()
        features = []
        page = 1

        if max_records != None:
            page_size = min(max_records, 1000)
        else:
            page_size = 1000

        while max_records == None or len(features) < max_records:

            if footprint[0] == 'shape':
                shape = footprint[1]
                json = requests.get(
                    f"https://catalogue.dataspace.copernicus.eu/resto/api/collections/{self.collection}/search.json?startDate={start_date}&completionDate={end_date}&geometry={shape}&productType={self.processing_level}&cloudCover=[0,{cloudcover}]&maxRecords={page_size}&page={page}"
                ).json()

            elif footprint[0] == 'tileid':

                tile_id_list = footprint[1]
                json = {'features':[]}

                for i, tile_id in enumerate(tile_id_list):
                    response = requests.get(
                        f"https://catalogue.dataspace.copernicus.eu/resto/api/collections/{self.collection}/search.json?startDate={start_date}&completionDate={end_date}&productType={self.processing_level}&cloudCover=[0,{cloudcover}]&maxRecords={page_size}&page={page}&tileId={tile_id}"
                        ).json()
                    json['features'].extend(response.get('features', []))

            print('## Query finished!')
            print(f"# Found {len(json['features'])} results")
            
            total_results = json["properties"]["totalResults"]
            if total_results == None:
                total_results = len(json["features"])

            if not max_records == None:
                max_records = min(max_records, total_results)
            else:
                max_records = total_results
            
            features = features + json["features"]
            page += 1

        return features[0:(max_records)]
    
    def stream_to_dir(self, feature, dir):
        url = feature.get("properties", {}).get("services", {}).get("download", {}).get("url")

        if not url:
            print("No download url found in feature, skipping...")
            return
            
        session = self.__get_authenticated_session()
        response = session.head(url, allow_redirects = False)
        while response.status_code in range(300, 399):
            url = response.headers['Location']
            response = session.head(url, allow_redirects=False)

        res = session.get(url, stream = True)
        res.raise_for_status()

        m = Message()
        m['content-type'] = res.headers["content-disposition"]
        filename = m.get_param('filename')

        content_length = int(res.headers["content-length"])
        progress = 0

        filesize = str(round(content_length / 1024 / 1024, 2)) + " MiB"
        prev_status = 0
        with open(os.path.join(dir, filename), 'wb') as f:
            for chunk in res.iter_content(chunk_size=1024 * 512):
                progress += len(chunk)
                status = round((progress / content_length) * 100, 2)

                if status - prev_status > 2:
                    prev_status = status
                    print(f"File: {filename} ({filesize})".ljust(120, '.') + f" Progress: {status}%")

                f.write(chunk)
        return os.path.join(dir, filename)
    
    # TODO: implement error handling to avoid crashing worker thread
    def download_feature(self, feature, dir, monitor = None):
        url = feature.get("properties", {}).get("services", {}).get("download", {}).get("url")

        if not url:
            print("No download url found in feature, skipping...")
            return
            
        session = self.__get_authenticated_session()
        response = session.head(url, allow_redirects = False)
        while response.status_code in range(300, 399):
            url = response.headers['Location']
            response = session.head(url, allow_redirects=False)

        res = session.get(url, stream = True)
        res.raise_for_status() # :)

        m = Message()
        m['content-type'] = res.headers["content-disposition"]
        filename = m.get_param('filename')

        content_length = int(res.headers["content-length"])
        progress = 0

        filesize = str(round(content_length / 1024 / 1024, 2)) + " MiB"
        with open(os.path.join(dir, filename), 'wb') as f:
            for chunk in res.iter_content(chunk_size=1024 * 512):
                progress += len(chunk)
                status = round((progress / content_length) * 100, 2)

                if monitor == None:
                    print(f"File: {filename} ({filesize})".ljust(120, '.') + f" Progress: {status}%")
                else:
                    monitor.update_status(filename, filesize, status)

                f.write(chunk)
        return os.path.join(dir, filename)

    def download_features(self, feature_list, dir, num_threads = 4):
        with StatusMonitor() as monitor:
            with ThreadPoolExecutor(max_workers = num_threads) as e:
                results = e.map(self.__call, [['download_feature', feature, dir, monitor] for feature in feature_list])
                result_list = [result for result in results]
        return result_list

    def __get_authenticated_session(self):
        session = requests.Session()
        self.__refresh_tokens()
        session.headers.update({'Authorization': f'Bearer {self.__access_token}'})
        return session
    
    def __get_netrc_credentials(self):
        info = netrc.netrc()
        auth = info.authenticators(self.__token_endpoint)

        if auth:
            login, _, password = auth
            return (login, password)
        else:
            return None

    def __refresh_tokens(self):
        if self.__refresh_token == None or self.__refresh_token_expires < datetime.now():
            print("Performing password token exchange..")
            res = self.__get_token({"username": self.__credentials[0], "password": self.__credentials[1], "grant_type": "password"})
        elif self.__access_token_expires < datetime.now():
            print("Performing refresh token exchange..")
            res = self.__get_token({"grant_type": "refresh_token", "refresh_token": self.__refresh_token})
        else:
            return

        self.__access_token = res["access_token"]
        self.__access_token_expires = datetime.now() + timedelta(seconds = res["expires_in"])
        self.__refresh_token = res["refresh_token"]
        self.__refresh_token_expires = datetime.now() + timedelta(seconds = res["refresh_expires_in"])

    # TODO: implement __get_token as a future
    def __get_token(self, payload):
        try:
            res = requests.post(self.__token_endpoint, data={"client_id": "cdse-public", **payload})
            res.raise_for_status()
            return res.json()
        except Exception as e:
            raise Exception(f"Token creation failed. Reponse from the server was: {res.json()}")

    def process_footprint(footprint):
        if isinstance(footprint, list):
            print('## Querying by tile ID')
            return ["tileid", footprint]
        elif isinstance(footprint, str):
            print('## Querying by shape file')
            return ["shape", CDSE.convert_to_odata_polygon(footprint)]
        else:
            raise Exception('## Footprint must be either path to shape file or tileid list!')
        
    def convert_to_odata_polygon(footprint):
        footprint = gpd.read_file(footprint).geometry[0]
        exterior = footprint.exterior
        coordinates = list(exterior.coords)
        odata_str = "POLYGON((" + ", ".join(" ".join(map(str, coord)) for coord in coordinates) + "))"
        return(odata_str)
    
    def __validate_required_params_present(self):
        params = ["collection", "processing_level"]
        for param in params:
            if getattr(self, param) == None:
                raise Exception(f"Please specify param ´{param}´ by calling CDSE#set_{param}")

    def __is_interactive():
        return sys.stdin and sys.stdin.isatty()
    
    def __call(self, call_definition):
        method, *args = call_definition
        for i in range(0, 3):
            try:
                getattr(self, method)(*args)
            except:
                print("Got an error. Sleeping 60, then retrying...")
                time.sleep(60)
                continue
            break

import time
class StatusMonitor(threading.Thread):
    __is_running = True
    __lines = {}
    __done_lines = []
    __progress_lines = 0

    __line_length = 120


    def stop(self):
        self.__is_running = False

    def update_status(self, filename, size, progress):
        if progress < 100:
            self.__lines[filename] = (filename, size, progress)
        else:
            self.__done_lines.append((filename, size, progress))
            del self.__lines[filename]

    def run(self):
        while True:
            time.sleep(0.5)
            if self.__is_running == False:
                break

            if len(self.__lines.values()) == 0:
                continue

            self.__clear_progress_lines()
            self.__print_done_lines()
            self.__print_progress_lines()

        print("")

    def __print_done_lines(self):
        while len(self.__done_lines) > 0:
            filename, _, _ = self.__done_lines.pop()
            print(f"- {filename} done".ljust(self.__line_length, " "))

    def __clear_progress_lines(self):
        #print(f"\x1B[{self.__progress_lines}A", end='')
        sys.stdout.write("\033[K")
        for _ in range(self.__progress_lines):
            sys.stdout.write("\033[F\033[K")

    def __print_progress_lines(self):
        lines_to_print = ["◼" * self.__line_length]
        lines_to_print += [self.__format_line(line) for line in self.__lines.values()]
        lines_to_print += ["◼" * self.__line_length]

        self.__progress_lines = len(lines_to_print)
        print("\n".join(lines_to_print))
    
    def __format_line(self, line):
        (filename, size, progress) = line
        return f"{filename} ({size})".ljust(self.__line_length - 7, ".") + f"{progress}%".rjust(7, ".")
    
    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
