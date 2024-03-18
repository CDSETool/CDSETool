import pytest
import json

from cdsetool.download import DownloadResult


def test_download_success(feature):
    res = DownloadResult(True, feature, "filename.zip", "Downloaded successfully")
    assert res.success is True
    assert res.filename == "filename.zip"
    assert res.message == "Downloaded successfully"
    assert (
        str(res)
        == f"Downloaded {feature.get('id')} to filename.zip: Downloaded successfully"
    )


def test_download_failure(feature):
    res = DownloadResult(False, feature, "filename.tar.gz", "something happened")
    assert res.success is False
    assert res.filename == "filename.tar.gz"
    assert res.message == "something happened"
    assert str(res) == f"Failed to download {feature.get('id')}: something happened"


@pytest.fixture
def feature():
    with open("tests/download/mock/feature.json") as f:
        return json.load(f)
