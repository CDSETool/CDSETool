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
from pathlib import Path
from typing import Any, Dict, Generator, List, Union
from xml.etree import ElementTree as ET

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

MANIFEST_FILENAMES = {
    "SENTINEL-1": "manifest.safe",
    "SENTINEL-2": "manifest.safe",
    "SENTINEL-3": "manifest.xml",
}


def filter_files(manifest_file: str, pattern: str, exclude: bool = False) -> List[str]:
    """
    Filter a product's files, listed in its manifest, based on a given pattern.

    Returns a list of files paths within the product bundle.

    If "exclude" is set to False, only files that match pattern are returned. If it's
    set to True, only files that do not match pattern are returned.
    """
    paths = []
    xmldoc = ET.parse(manifest_file)

    if os.path.basename(manifest_file) == "manifest.safe":
        data_obj_section_elem = xmldoc.find("dataObjectSection")
        for elem in data_obj_section_elem.iterfind("dataObject"):  # type: ignore
            path = Path(elem.find("byteStream/fileLocation").attrib["href"])  # type: ignore
            paths.append(path)

    elif os.path.basename(manifest_file) == "manifest.xml":
        namespaces = {"ns": "http://www.eumetsat.int/sip"}
        data_section_elem = xmldoc.find("ns:dataSection", namespaces)
        for elem in data_section_elem.iterfind("ns:dataObject", namespaces):  # type: ignore
            path = Path(elem.find("ns:path", namespaces).text)  # type: ignore
            # Remove prefix present for some files in S3 manifests
            path = Path(*path.parts[-1:])
            paths.append(path)

    paths = [str(path) for path in paths if fnmatch.fnmatch(path, pattern) ^ exclude]

    return paths


def download_file(url: str, path: str, options: Dict[str, Any]) -> bool:
    """
    Download a single file.
    """
    log = _get_logger(options)
    filename = os.path.basename(path)

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
            with session.get(url, stream=True) as response:
                if response.status_code != 200:
                    log.warning(f"Status code {response.status_code}, retrying..")
                    time.sleep(60 * (1 + (random.random() / 4)))
                    continue

                status.set_filesize(int(response.headers["Content-Length"]))
                with open(path, "wb") as outfile:
                    # Server might not send all bytes specified by the
                    # Content-Length header before closing connection.
                    # Log as a warning and try again.
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
                return True

    log.error(f"Failed to download {filename}")
    return False


def download_feature(  # pylint: disable=too-many-return-statements
    feature, path: str, options: Union[Dict[str, Any], None] = None
) -> Union[str, None]:
    """
    Download a single feature.

    Returns the feature title.
    """
    options = options or {}
    log = _get_logger(options)
    temp_dir_usr = _get_temp_dir(options)
    title = feature.get("properties").get("title")
    filename = title + ("" if "filter_pattern" in options else ".zip")
    result_path = os.path.join(path, filename)

    if not options.get("overwrite_existing", False) and os.path.exists(result_path):
        log.debug(f"File {result_path} already exists, skipping..")
        return filename

    with tempfile.TemporaryDirectory(
        prefix=f"{title}____", dir=temp_dir_usr
    ) as temp_dir:
        if options.get("filter_pattern"):
            # Download filtered files within product from OData API's URLs

            temp_product_path = os.path.join(temp_dir, title)
            os.makedirs(temp_product_path, exist_ok=True)

            # Download manifest file
            try:
                manifest_filename = MANIFEST_FILENAMES[
                    feature["properties"]["collection"]
                ]
            except KeyError:
                log.error(
                    "Downloading specific files within product bundle with node "
                    f"filtering is not supported for this collection type: {title}"
                )
                return None
            manifest_file = os.path.join(temp_product_path, manifest_filename)
            result = download_file(
                _get_odata_url(feature["id"], title, manifest_filename),
                manifest_file,
                options,
            )
            if not result:
                log.error(f"Failed to download {manifest_filename} in {title}")
                return None

            # List files that match pattern based on manifest file contents
            try:
                filtered_files = filter_files(manifest_file, options["filter_pattern"])
            except Exception as e:  # pylint: disable=broad-exception-caught
                log.error(f"Failed to filter files in {title}: {e}")
                return None

            for filtered_file in filtered_files:
                output_file = os.path.join(temp_product_path, filtered_file)
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                result = download_file(
                    _get_odata_url(feature["id"], title, filtered_file),
                    output_file,
                    options,
                )
                if not result:
                    log.error(f"Failed to download all selected files in {title}")
                    return None

            # Move downloaded files to output dir
            os.makedirs(path, exist_ok=True)
            shutil.move(temp_product_path, path)

            return title

        url = _get_feature_url(feature)

        if not url or not title:
            log.debug(f"Bad URL ('{url}') or title ('{title}')")
            return None

        temp_file = os.path.join(temp_dir, filename)
        result = download_file(url, temp_file, options)
        if result:
            shutil.copy(temp_file, result_path)

        return filename


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
        return download_feature(feature, path, options)

    yield from _concurrent_process(
        _download_feature, features, options.get("concurrency", 1)
    )

    options["monitor"].stop()


def _get_feature_url(feature) -> str:
    return feature.get("properties").get("services").get("download").get("url")


def _get_odata_url(product_id: str, product_name: str, href: str) -> str:
    """
    Convert href, describing file location in manifest file, to an OData download URL.
    """
    odata_url = "https://download.dataspace.copernicus.eu/odata/v1"
    path = "/".join([f"Nodes({item})" for item in href.split("/")])
    return f"{odata_url}/Products({product_id})/Nodes({product_name})/{path}/$value"


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
