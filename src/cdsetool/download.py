"""
Download features from a Copernicus Data Space Ecosystem OpenSearch API result

Provides a function to download a single feature, a function to download all features
in a result set, and a function to download specific files in a given feature using
node filtering.
"""

import fnmatch
import os
import random
import shutil
import tempfile
import time
from typing import Any, Dict, Generator, List, Union
from xml.etree import ElementTree as etree

from requests import Session
from requests.exceptions import ChunkedEncodingError
from urllib3.exceptions import ProtocolError

from cdsetool._processing import _concurrent_process
from cdsetool.credentials import (
    Credentials,
    TokenClientConnectionError,
    TokenExpiredSignatureError,
)
from cdsetool.logger import NoopLogger
from cdsetool.monitor import NoopMonitor, StatusMonitor
from cdsetool.query import FeatureQuery

MANIFEST_BASENAMES = {
    "SENTINEL-1": "manifest.safe",
    "SENTINEL-2": "manifest.safe",
    "SENTINEL-3": "manifest.xml",
}


def _href_to_url(odata_url: str, product_id: str, product_name: str, href: str) -> str:
    """
    Convert href, describing file location in manifest file, to an OData download URL.
    """
    path = "/".join([f"Nodes({item})" for item in href.split("/")])
    return f"{odata_url}/Products({product_id})/Nodes({product_name})/{path}/$value"


def filter_files(
    manifest_file: str, pattern: str, exclude: bool = False
) -> List[Dict[str, Any]]:
    """
    Filter a product's files, listed in its manifest, based on a given pattern.

    Returns a list of files paths within the product bundle.

    If "exclude" is set to False, only files that match pattern are returned. If it's
    set to True, only files that do not match pattern are returned.
    """
    paths = []
    xmldoc = etree.parse(manifest_file)

    if os.path.basename(manifest_file) == "manifest.safe":
        data_obj_section_elem = xmldoc.find("dataObjectSection")
        for elem in data_obj_section_elem.iterfind("dataObject"):
            path = elem.find("byteStream/fileLocation").attrib["href"]
            path = path[2:]  # Remove "./" prefix present in S2 and S3 manifests
            match = fnmatch.fnmatch(path.lower(), pattern)
            if match and not exclude or exclude and not match:
                paths.append(path)

    elif os.path.basename(manifest_file) == "manifest.xml":
        namespaces = {"ns": "http://www.eumetsat.int/sip"}
        data_section_elem = xmldoc.find("ns:dataSection", namespaces)
        for elem in data_section_elem.iterfind("ns:dataObject", namespaces):
            path = elem.find("ns:path", namespaces).text
            path = "/".join(
                path.split("/")[1:]
            )  # Remove product name prefix in S3 manifests
            match = fnmatch.fnmatch(path.lower(), pattern)
            if match and not exclude or exclude and not match:
                paths.append(path)

    return paths


def download_file(url: str, path: str, options: Dict[str, Any]) -> Union[str, None]:
    """
    Download a single file.

    Returns local path of downloaded file, or None in case of failure.
    """
    log = _get_logger(options)
    basename = os.path.basename(path)

    with _get_monitor(options).status() as status:
        status.set_filename(basename)
        attempts = 0
        while attempts < 10:
            attempts += 1
            try:
                session = _get_credentials(options).get_session()
            except TokenClientConnectionError:
                log.warning("Token client connection failed, retrying..")
                continue
            except TokenExpiredSignatureError:
                log.warning("Token signature expired, retrying..")
                continue
            url = _follow_redirect(url, session)
            with session.get(url, stream=True) as response:
                if response.status_code != 200:
                    log.warning(f"Status code {response.status_code}, retrying..")
                    time.sleep(60 * (1 + (random.random() / 4)))
                    continue

                status.set_filesize(int(response.headers["Content-Length"]))
                with open(path, "wb") as outfile:
                    try:
                        for chunk in response.iter_content(chunk_size=1024 * 1024 * 5):
                            outfile.write(chunk)
                            status.add_progress(len(chunk))
                    except (
                        ChunkedEncodingError,
                        ConnectionResetError,
                        ProtocolError,
                    ) as e:
                        log.warning(e)
                        continue
                return path

    log.error(f"Failed to download {basename}")
    return None


def download_nodes(
    feature,
    path: str,
    filter_pattern: str,
    options: Union[Dict[str, Any], None] = None,
) -> Union[str, None]:
    """
    Download specific files within a feature using OData API's node functionality.

    Returns the product name, or None in case of failure.
    """
    options = options or {}
    log = _get_logger(options)
    temp_dir_usr = _get_temp_dir(options)

    odata_url = "https://download.dataspace.copernicus.eu/odata/v1"
    product_name = feature["properties"]["title"]

    with tempfile.TemporaryDirectory(prefix=product_name, dir=temp_dir_usr) as temp_dir:
        temp_product_path = os.path.join(temp_dir, product_name)
        os.makedirs(temp_product_path, exist_ok=True)

        # Download manifest file
        manifest_basename = MANIFEST_BASENAMES[feature["properties"]["collection"]]
        manifest_file = os.path.join(temp_product_path, manifest_basename)
        manifest_file = download_file(
            _href_to_url(odata_url, feature["id"], product_name, manifest_basename),
            manifest_file,
            options,
        )
        if manifest_file is None:
            log.error(f"Failed to download {manifest_basename} in {product_name}")
            return None

        # List files that match pattern based on manifest file contents
        try:
            filtered_files = filter_files(manifest_file, filter_pattern)
        except Exception as e:
            log.error(f"Failed to filter files in {product_name}: {e}")
            return None

        for filtered_file in filtered_files:
            output_file = os.path.join(
                temp_product_path,
                filtered_file,  # TODO: Check if this needs to be generalized
            )
            os.makedirs(os.path.dirname(output_file), exist_ok=True)

            output_file = download_file(
                _href_to_url(
                    odata_url,
                    feature["id"],
                    product_name,
                    filtered_file,  # TODO: Check if this needs to be generalized
                ),
                output_file,
                options,
            )
            if output_file is None:
                log.error(f"Failed to download all selected files in {product_name}")
                return None

        # Move downloaded files to output dir
        os.makedirs(path, exist_ok=True)
        shutil.move(temp_product_path, path)

    return product_name


def download_feature(
    feature, path: str, options: Union[Dict[str, Any], None] = None
) -> Union[str, None]:
    """
    Download a single feature

    Returns the feature ID
    """
    options = options or {}
    log = _get_logger(options)
    url = _get_feature_url(feature)
    title = feature.get("properties").get("title")
    temp_dir_usr = _get_temp_dir(options)

    if not url or not title:
        log.debug(f"Bad URL ('{url}') or title ('{title}')")
        return None

    filename = title + ".zip"
    result_path = os.path.join(path, filename)

    if not options.get("overwrite_existing", False) and os.path.exists(result_path):
        log.debug(f"File {result_path} already exists, skipping..")
        return filename

    with _get_monitor(options).status() as status:
        status.set_filename(filename)
        attempts = 0
        while attempts < 10:
            attempts += 1
            # Always get a new session, credentials might have expired.
            try:
                session = _get_credentials(options).get_session()
            except TokenClientConnectionError:
                log.warning("Token client connection failed, retrying..")
                continue
            except TokenExpiredSignatureError:
                log.warning("Token signature expired, retrying..")
                continue
            url = _follow_redirect(url, session)
            name_dir_prefix = filename.replace(".zip", "____")
            with (
                session.get(url, stream=True) as response,
                tempfile.TemporaryDirectory(
                    prefix=name_dir_prefix, dir=temp_dir_usr
                ) as temp_dir,
            ):
                if response.status_code != 200:
                    log.warning(f"Status code {response.status_code}, retrying..")
                    time.sleep(60 * (1 + (random.random() / 4)))
                    continue

                status.set_filesize(int(response.headers["Content-Length"]))
                tmp_file = os.path.join(temp_dir, "download.zip")
                with open(tmp_file, "wb") as file:
                    # Server might not send all bytes specified by the
                    # Content-Length header before closing connection.
                    # Log as a warning and try again.
                    try:
                        for chunk in response.iter_content(chunk_size=1024 * 1024 * 5):
                            file.write(chunk)
                            status.add_progress(len(chunk))
                    except (
                        ChunkedEncodingError,
                        ConnectionResetError,
                        ProtocolError,
                    ) as e:
                        log.warning(e)
                        continue
                # Close file before copy so all buffers are flushed.
                shutil.copy(tmp_file, result_path)
                return filename
    log.error(f"Failed to download {filename}")
    return None


def download_features(
    features: FeatureQuery, path: str, options: Union[Dict[str, Any], None] = None
) -> Generator[Union[str, None], None, None]:
    """
    Generator function that downloads all features in a result set

    Feature IDs are yielded as they are downloaded
    """
    options = options or {}

    options["credentials"] = _get_credentials(options)
    options["logger"] = _get_logger(options)

    options["monitor"] = _get_monitor(options)
    options["monitor"].start()

    def _download_feature(feature) -> Union[str, None]:
        if options["filter_pattern"] is not None:
            return download_nodes(feature, path, options["filter_pattern"], options)
        else:
            return download_feature(feature, path, options)

    yield from _concurrent_process(
        _download_feature, features, options.get("concurrency", 1)
    )

    options["monitor"].stop()


def _get_feature_url(feature) -> str:
    return feature.get("properties").get("services").get("download").get("url")


def _follow_redirect(url: str, session: Session) -> str:
    response = session.head(url, allow_redirects=False)
    while response.status_code in range(300, 400):
        url = response.headers["Location"]
        response = session.head(url, allow_redirects=False)

    return url


def _get_logger(options: Dict) -> NoopLogger:
    return options.get("logger") or NoopLogger()


def _get_monitor(options: Dict) -> Union[StatusMonitor, NoopMonitor]:
    return options.get("monitor") or NoopMonitor()


def _get_credentials(options: Dict) -> Credentials:
    return options.get("credentials") or Credentials(
        proxies=options.get("proxies", None)
    )


def _get_temp_dir(options: Dict) -> Union[str, None]:
    return options.get("tmpdir") or None
