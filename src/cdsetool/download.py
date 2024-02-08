"""
Download features from a Copernicus Data Space Ecosystem OpenSearch API result

Provides a function to download a single feature, and a function to download
all features in a result set.
"""
import os
import random
import tempfile
import hashlib
import time
import shutil
from pathlib import Path
from src.cdsetool.query import get_odata_from_product_title
from src.cdsetool._processing import _concurrent_process
from src.cdsetool.credentials import Credentials
from src.cdsetool.monitor import NoopMonitor
from blake3 import blake3

class InvalidChecksumError(Exception):
    """MD5 checksum of a local file does not match the one from the server."""
    pass

def download_feature(feature, path, options = None,attempt = 0):
    """
    Download a single feature

    Returns the feature ID
    """
    if attempt  == options.get("retries", 3):
        """Tries to download a partial file """
        return feature.get("id")
    options = options or {}
    url = _get_feature_url(feature)
    filename = feature.get("properties").get("title")

    if not url or not filename:
        return feature.get("id")

    overwrite = options.get("overwrite_existing", False)
    continue_existing = options.get("continue_existing", True)

    if not os.path.exists(Path(path)):
        os.makedirs(path)
    result_path = os.path.join(path, filename.replace(".SAFE", ".zip"))
    temp_path =  os.path.join(path, filename.replace(".SAFE", ".zip.incomplete"))
    if not overwrite and os.path.exists(result_path):
        return feature.get("id")
    size_temp = 0
    if not continue_existing and os.path.exists(temp_path):
        """
        if temp path exists take the size already downloaded and use it in the headers
        """
        size_temp = temp_path.stat().st_size
    elif overwrite and os.path.exists(temp_path):
        os.remove(temp_path)
    with _get_monitor(options).status() as status:
        status.set_filename(filename)
        session = _get_credentials(options).get_session()
        session = _set_proxy(options,session) # here set the proxy
        session = _set_partial_download(session=session,size=size_temp) # here sets the header for partial downloads
        url = _follow_redirect(url, session)
        response = _retry_backoff(url, session)
        content_length = int(response.headers["Content-Length"])
        status.set_filesize(content_length)
        with open(temp_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024 * 1024 * 5):
                if not chunk:
                    continue

                file.write(chunk)
                status.add_progress(len(chunk))
                
    valid_checksum = validity_check(result_path,temp_path,feature,options)
    if not valid_checksum:
        """Partial dowload call back download feature"""
        return download_feature(feature,path,options,attempt+1)
    else:
        return feature.get("id")

def validity_check(path,temp_path,product_info, verify_checksum: bool = True,options={}):
    size = os.path.getsize(temp_path)
    content_length = product_info.get("properties").get("services").get("download").get("size")
    if size > content_length:
            #Dowloaded more than the size of the file. Delete the downloaded and download the file from the beginning
            os.remove(temp_path)
            return False
    elif size == content_length:
        # Check integrity with MD5 checksum
        if verify_checksum:
            checksum_comparison  = _checksum_compare(temp_path, product_info)
            if checksum_comparison == None:
                # No data available for the checksum, the download it's leaved in incomplete format
                return True
            elif checksum_comparison:
                shutil.move(temp_path, path)
                return True
            else:
                # Checksum failed, download the file from the beginning
                # TODO here we should decide if it't better to retry or not
                os.remove(temp_path)
                return False
    else:
        """Partial dowload call back download feature"""
        return False

def _checksum_compare(temp_path, product_info):
    """Compare a given MD5 checksum with one calculated from a file."""
    checksum = None
    algo = None
    filename = product_info.get("properties").get("title")
    if "Checksum" in product_info:
        # The intial query was made with the checksum option.
        checksum_list =product_info.get("Checksum")
        algo, checksum =from_checksum_list_to_checksum(checksum_list)
    else:
        odata_response = get_odata_from_product_title(filename)
        if 'value' in odata_response:
            if len(odata_response['value']) ==1:
                # odata['value'] is a list if there is more then one element for a single product
                # there is a error on the odata I guess 
                if 'Checksum' in odata_response['value'][0]:
                    checksum_list = odata_response['value'][0]['Checksum']
                    algo, checksum =from_checksum_list_to_checksum(checksum_list)
    if checksum == None:
        return None # no checksum available, so we skip verifying
    file_path = temp_path
    with open(file_path, "rb") as f:
        while True:
            block_data = f.read(8192)
            if not block_data:
                break
            algo.update(block_data)
            # status.add_progress(block_data)
    return algo.hexdigest().lower() == checksum.lower() # checksum available, so we give the verify checksum

def from_checksum_list_to_checksum(checksum_list):
    if len(checksum_list) == 0:
        return None, None # no checksum available, so we skip verifying
    algo, checksum = checksum_list[0].get("Algorithm"), checksum_list[0].get("Value")
    algo = {
        "sha3-256": hashlib.sha3_256(),
        "MD5": hashlib.md5(),
        "BLAKE3": blake3() # https://pypi.org/project/blake3/
    }[algo]
    return algo,checksum

def download_features(features, path, options=None):
    """
    Generator function that downloads all features in a result set

    Feature IDs are yielded as they are downloaded
    """
    options = options or {}

    options["credentials"] = _get_credentials(options)

    options["monitor"] = _get_monitor(options)
    options["monitor"].start()

    def _download_feature(feature):
        return download_feature(feature, path, options)

    for feature in _concurrent_process(
        _download_feature, features, options.get("concurrency", 1)
    ):
        yield feature

    options["monitor"].stop()


def _get_feature_url(feature):
    return feature.get("properties").get("services").get("download").get("url")


def _follow_redirect(url, session):
    response = session.head(url, allow_redirects=False)
    while response.status_code in range(300, 400):
        url = response.headers["Location"]
        response = session.head(url, allow_redirects=False)
    return url

def _retry_backoff(url, session):
    response = session.get(url, stream=True)
    while response.status_code != 200:
        time.sleep(60 * (1 + (random.random() / 4)))
        response = session.get(url, stream=True)

    return response

def _set_proxy(options,session):
    proxies = options.get("proxies", {})
    if proxies != {}:
        session.proxies.update(proxies)
    return session

def _set_partial_download(session,size = 0):
    if size !=0:
        session.headers.update({"Range": "bytes={}-".format(size)})
    return session

def _get_monitor(options):
    return options.get("monitor") or NoopMonitor()

def _get_credentials(options):
    return options.get("credentials") or Credentials()
