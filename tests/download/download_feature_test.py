import pytest
import json
import tempfile
import os

from cdsetool.download import DownloadSuccess, DownloadFailure, download_feature


def test_feature_missing_url(tmpdir, feature):
    del feature["properties"]["services"]["download"]["url"]

    res = download_feature(feature, tmpdir)
    assert isinstance(res, DownloadFailure)
    assert (
        str(res)
        == f"Failed to download {feature.get('id')}: Feature has no download URL"
    )


def test_feature_missing_title(tmpdir, feature):
    del feature["properties"]["title"]

    res = download_feature(feature, tmpdir)
    assert isinstance(res, DownloadFailure)
    assert str(res) == f"Failed to download {feature.get('id')}: Feature has no title"


def test_skip_existing_file(tmpdir, feature):
    tmpfile = os.path.join(
        tmpdir, feature["properties"]["title"].replace(".SAFE", ".zip")
    )
    with open(tmpfile, "w") as f:
        f.write("somebytes")

    res = download_feature(feature, tmpdir)
    assert isinstance(res, DownloadSuccess)
    assert res.filename == tmpfile
    assert str(res) == f"Downloaded {feature.get('id')} to {tmpfile}"


@pytest.fixture()
def feature():
    with open("tests/download/mock/feature.json") as f:
        return json.load(f)
