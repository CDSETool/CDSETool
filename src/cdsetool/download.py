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
    "SENTINEL-3": "xfdumanifest.xml",
}


def filter_files(
    manifest_file: Path, pattern: Union[str, None], exclude: bool = False
) -> List[Path] | None:
    """
    Filter a product's files, listed in its manifest, based on a given pattern.

    Returns a list of file paths within the product bundle.

    All files not matching the pattern are returned if "exclude" is set to true.
    """

    def read_paths_from_manifest(manifest_file: Path) -> List[Path] | None:
        xmldoc = ET.parse(manifest_file)
        section = xmldoc.find("dataObjectSection")
        if section is None:
            return None
        paths = []
        for elem in section.iterfind("dataObject"):
            obj = elem.find("byteStream/fileLocation")
            if obj is None:
                return None
            obj = obj.get("href")
            if obj is None:
                return None
            paths.append(Path(obj))
        return paths

    if pattern is None:
        return []
    paths = read_paths_from_manifest(manifest_file)
    if paths is None:
        return None
    return [path for path in paths if fnmatch.fnmatch(str(path), pattern) ^ exclude]


def download_file(url: str, path: Path, options: Dict[str, Any]) -> bool:
    """
    Download a single file.
    """
    log = _get_logger(options)
    filename = path.name

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
    collection = feature.get("properties").get("collection")
    download_full = "filter_pattern" not in options

    try:
        manifest_filename = "" if download_full else MANIFEST_FILENAMES[collection]
    except KeyError:
        log.error(
            f"No support for downloading individual files in {collection} products"
        )
        return None

    # Prepare to download full product, or manifest file if filter_pattern is used
    filename = title + ".zip" if download_full else manifest_filename
    url = (
        _get_feature_url(feature)
        if download_full
        else _get_odata_url(feature["id"], title, filename)
    )
    if not url or not title:
        log.debug(f"Bad URL ('{url}') or title ('{title}')")
        return None

    result_path = os.path.join(path, filename if download_full else title)
    if not options.get("overwrite_existing", False) and os.path.exists(result_path):
        log.debug(f"File {result_path} already exists, skipping..")
        return os.path.basename(result_path)

    with tempfile.TemporaryDirectory(
        prefix=f"{title}____", dir=temp_dir_usr
    ) as temp_dir:
        temp_file = os.path.join(temp_dir, filename)
        if not download_file(url, Path(temp_file), options):
            return None

        # If filter_pattern is used, list matching files based on manifest contents
        temp_product_path = os.path.join(temp_dir, title)
        filtered_files = filter_files(Path(temp_file), options.get("filter_pattern"))
        if filtered_files is None:
            log.error(f"Failed to parse manifest file for {title}")
            return None
        for file in filtered_files:
            output_file = os.path.join(temp_product_path, file)
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            if not download_file(
                _get_odata_url(feature["id"], title, str(file)),
                Path(output_file),
                options,
            ):
                log.error(f"Failed to download {file} from {title}")
                return None

        # Move downloaded files to output dir
        if download_full or filtered_files:
            os.makedirs(path, exist_ok=True)
            shutil.move(temp_file if download_full else temp_product_path, path)

        return filename if download_full else title


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
