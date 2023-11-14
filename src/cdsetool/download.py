from cdsetool.credentials import Credentials
from cdsetool.monitor import NoopMonitor
from cdsetool._processing import _concurrent_process
import os
import time
import random
import tempfile


def download_feature(feature, path, options={}):
    url = _get_feature_url(feature)
    filename = feature.get("properties").get("title")
    file = os.path.join(path, filename)

    if not url or not filename:
        return feature.get("id")

    # if os.path.exists(file):
    #     return feature.get("id")

    with options.get("monitor", NoopMonitor()).status() as s:
        s.set_filename(filename)

        session = options.get("credentials", Credentials()).get_session()
        url = _follow_redirect(url, session)
        response = _retry_backoff(url, session)

        content_length = int(response.headers["Content-Length"])

        s.set_filesize(content_length)

        fd, tmp = tempfile.mkstemp()
        with open(tmp, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024 * 5):
                if not chunk:
                    continue

                f.write(chunk)
                s.update_progress(len(chunk))

        os.close(fd)
        os.rename(tmp, file)

    return feature.get("id")


def download_features(features, path, options={}):
    monitor = options.get("monitor", NoopMonitor)()
    monitor.start()

    options["monitor"] = monitor

    def _download_feature(feature):
        return download_feature(feature, path, options)

    return _concurrent_process(
        _download_feature, features, options.get("concurrency", 1)
    )


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
