import pytest
import json

from cdsetool.download import DownloadSuccess, DownloadFailure


def test_download_success():
    assert (
        str(DownloadSuccess(_feature(), "myfilepath"))
        == "Downloaded 171fd093-b1bc-4528-910b-73156cd0b5d3 to myfilepath"
    )


def test_download_failure():
    assert (
        str(DownloadFailure(_feature(), "something happened"))
        == "Failed to download 171fd093-b1bc-4528-910b-73156cd0b5d3: something happened"
    )


def _feature():
    with open("tests/download/mock/feature.json") as f:
        return json.load(f)
