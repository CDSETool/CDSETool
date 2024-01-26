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
from src.cdsetool._processing import _concurrent_process
from src.cdsetool.credentials import Credentials
from src.cdsetool.monitor import NoopMonitor

class InvalidChecksumError(Exception):
    """MD5 checksum of a local file does not match the one from the server."""

    pass

def download_feature(feature, path, options=None):
    """
    Download a single feature

    Returns the feature ID
    """
    options = options or {}
    url = _get_feature_url(feature)
    filename = feature.get("properties").get("title")

    if not url or not filename:
        return feature.get("id")

    overwrite = options.get("overwrite_existing", False)
    continue_ = options.get("continue_existing", True)

    if not os.path.exists(Path(path)):
        os.makedirs(path)
    result_path = os.path.join(path, filename.replace(".SAFE", ".zip"))
    temp_path =  Path(path) / filename.replace(".SAFE", ".zip.incomplete") #os.path.join(path, filename.replace(".SAFE", ".zip.incomplete"))
    if not overwrite and os.path.exists(result_path):
        return feature.get("id")
    
    if not continue_ and os.path.exists(temp_path):
        #TODO decide what to do with partlial files, 
        """
        it should be appropriate to restart the download from the
        from where it let off istead of the beginning.
        The current version remove the partial file and restart the download
        from the beginning.
        """
        return feature.get("id")
    if os.path.exists(temp_path):
        os.remove(temp_path)
    with _get_monitor(options).status() as status:
        status.set_filename(filename)
        session = _get_credentials(options).get_session()
        url = _follow_redirect(url, session)
        response = _retry_backoff(url, session)
        content_length = int(response.headers["Content-Length"])

        status.set_filesize(content_length)

        fd, tmp = tempfile.mkstemp()  # pylint: disable=invalid-name
        with open(temp_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024 * 1024 * 5):
                if chunk:
                    file.write(chunk)
                    status.add_progress(len(chunk))
                
    validity_check(Path(result_path),temp_path,feature)
        # os.close(fd)
        # shutil.move(tmp, result_path)

    return feature.get("id")

def validity_check(path: Path,temp_path: Path,product_info, verify_checksum: bool = True):
    size = temp_path.stat().st_size
    content_length = product_info.get("properties").get("services").get("download").get("size")
    if size > content_length:
            temp_path.unlink()
            raise(f"Content length mismatch for {path.name}")
    elif size == content_length:
        # Check integrity with MD5 checksum
        if verify_checksum:
            if _checksum_compare(temp_path, product_info):
                shutil.move(temp_path, path)
            else:
                temp_path.unlink()
                raise(f"Checksum mismatch for file {path.name}")
    else:
        """TODO: handle partial downloads"""
        pass

def _checksum_compare(temp_path, product_info,block_size=2**13):
    """Compare a given MD5 checksum with one calculated from a file."""
    checksum = None
    algo = None
    filename = product_info.get("properties").get("title")
    if "Checksum" in product_info:
        checksum_list = product_info.get("Checksum")
        if len(checksum_list) >0:
            for checksum_dict in checksum_list:
                if "Algorithm" in checksum_dict:
                    algo = checksum_dict['Algorithm']
                    if algo == "sha3-256":
                        checksum = checksum_dict['Value']
                        algo = hashlib.sha3_256() 
                    elif algo == "MD5":
                        checksum = checksum_dict['Value']
                        algo = hashlib.md5()
                        break
                    elif algo == "BLAKE3":
                        checksum = checksum_dict['Value']
                        algo = hashlib.blake2b() # TODO ?
                else:
                    raise InvalidChecksumError("No checksum information found in product information. No Algorithm provided provided")
        else:
                    raise InvalidChecksumError("No checksum information found in product information. The Checksum list is empty")
    if checksum == None:
        raise InvalidChecksumError("No checksum algo provided is supported.")
    file_path = temp_path
    file_size = file_path.stat().st_size
    # with _get_monitor({}).status() as status:
    #     status.set_filename(f"{algo} - {filename}")
    #     status.set_filesize(file_size)
    with open(file_path, "rb") as f:
        while True:
            block_data = f.read(block_size)
            if not block_data:
                break
            algo.update(block_data)
            # status.add_progress(block_data)
    return algo.hexdigest().lower() == checksum.lower()

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


def _get_monitor(options):
    return options.get("monitor") or NoopMonitor()


def _get_credentials(options):
    return options.get("credentials") or Credentials()
