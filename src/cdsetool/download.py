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
from typing import Any, Dict, Generator, Union

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

    filename = title.replace(".SAFE", ".zip")
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
            with session.get(url, stream=True) as response, tempfile.TemporaryDirectory(
                prefix=name_dir_prefix, dir=temp_dir_usr
            ) as temp_dir:
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
