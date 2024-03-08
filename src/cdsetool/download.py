"""
Download features from a Copernicus Data Space Ecosystem OpenSearch API result

Provides a function to download a single feature, and a function to download
all features in a result set.
"""

import os
import random
import tempfile
import time
import shutil
import hashlib
from enum import Enum
from blake3 import blake3
from cdsetool._processing import _concurrent_process
from cdsetool.credentials import Credentials
from cdsetool.logger import NoopLogger
from cdsetool.monitor import NoopMonitor


class Validity(Enum):
    """
    Validity enum for the checksum
    """

    VALID = 1
    INVALID = 2
    IGNORE = 3
    CONTINUE = 4


def download_feature(feature, path, options=None):
    """
    Download a single feature

    Returns the feature ID
    """
    options = options or {}
    log = _get_logger(options)
    url = _get_feature_url(feature)
    title = feature.get("properties").get("title")

    if not url or not title:
        log.debug(f"Bad URL ('{url}') or title ('{title}')")
        return None

    filename = title.replace(".SAFE", ".zip")
    result_path = os.path.join(path, filename)

    if not options.get("overwrite_existing", False) and os.path.exists(result_path):
        log.debug(f"File {result_path} already exists, skipping..")
        return filename

    with _get_monitor(options).status() as status:
        status.set_filename(filename)

        session = _get_credentials(options).get_session()
        session = _set_proxy(options, session)  # here set the proxy
        url = _follow_redirect(url, session)
        response = _retry_backoff(url, session, options)

        content_length = int(response.headers["Content-Length"])

        status.set_filesize(content_length)

        fd, tmp = tempfile.mkstemp()  # pylint: disable=invalid-name
        with open(fd, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024 * 1024 * 5):
                file.write(chunk)
                status.add_progress(len(chunk))
        valid_checksum = validity_check(tmp, feature)
        if valid_checksum in (Validity.VALID, Validity.IGNORE):
            shutil.move(tmp, result_path)
        else:
            # TODO here to implement the logic what if the checksum is invalid?
            os.remove(tmp)
    return filename


def download_features(features, path, options=None):
    """
    Generator function that downloads all features in a result set

    Feature IDs are yielded as they are downloaded
    """
    options = options or {}

    options["credentials"] = _get_credentials(options)
    options["logger"] = _get_logger(options)

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


def _retry_backoff(url, session, options):
    response = session.get(url, stream=True)
    while response.status_code != 200:
        options["logger"].warning(f"Status code {response.status_code}, retrying..")
        time.sleep(60 * (1 + (random.random() / 4)))
        response = session.get(url, stream=True)

    return response


def _get_logger(options):
    return options.get("logger") or NoopLogger()


def _get_monitor(options):
    return options.get("monitor") or NoopMonitor()


def _get_credentials(options):
    return options.get("credentials") or Credentials()


def _set_proxy(options, session):
    proxies = options.get("proxies", {})
    if proxies != {}:
        session.proxies.update(proxies)
    return session


def validity_check(temp_path, product_info):
    """Given it's temp_path and metadata info checks if the data downloaded is valid"""
    size = os.path.getsize(temp_path)
    content_length = (
        product_info.get("properties").get("services").get("download").get("size")
    )
    if size > content_length:
        # Dowloaded more than the size of the file.
        return Validity.INVALID
    if size == content_length:
        checksum_comparison = _checksum_compare(temp_path, product_info)
        if checksum_comparison is None:
            # No data available for the checksum
            return Validity.IGNORE
        if checksum_comparison:
            return Validity.VALID
        # Checksum failed
        return Validity.INVALID
    # here it's a partial download logic to implement is TODO
    return Validity.CONTINUE


def _checksum_compare(temp_path, product_info):
    """Compare a given MD5 checksum with one calculated from a file."""
    checksum = None
    algo = None
    checksum_list = product_info.get("Checksum", [])
    algo, checksum = from_checksum_list_to_checksum(checksum_list)
    if checksum is None:
        # no checksum available
        return None
    with open(temp_path, "rb") as f:
        while True:
            block_data = f.read(8192)
            if not block_data:
                break
            algo.update(block_data)
    return algo.hexdigest().lower() == checksum.lower()


def from_checksum_list_to_checksum(checksum_list):
    """
    From a list of checksum (provided by ESA if provided)
    return the checksum and algo
    """
    if not checksum_list or len(checksum_list) == 0:
        # checksum odata is not provided in the metadata
        return None, None
    algo, checksum = checksum_list[0].get("Algorithm"), checksum_list[0].get("Value")
    algo = {
        "sha3-256": hashlib.sha3_256(),
        "MD5": hashlib.md5(),
        "BLAKE3": blake3(),
    }.get(algo)
    return algo, checksum
