"""Tests for CDSETool's download module."""

import logging
import os
from pathlib import Path
from typing import Any

import pytest
from cdsetool.download import (
    _get_odata_url,
    download_feature,
    download_file,
    filter_files,
)

from ..mock_auth import _mock_jwks, _mock_openid, _mock_token


def test_get_odata_url():
    product_id = "a6215824-704b-46d7-a2ec-efea4e468668"
    product_name = "S2B_MSIL1C_20241209T162609_N0511_R040_T17UPV_20241209T195414.SAFE"
    href = "path/to/resource.xml"
    expected_url = (
        "https://download.dataspace.copernicus.eu/odata/v1/"
        "Products(a6215824-704b-46d7-a2ec-efea4e468668)/"
        "Nodes(S2B_MSIL1C_20241209T162609_N0511_R040_T17UPV_20241209T195414.SAFE)/"
        "Nodes(path)/Nodes(to)/Nodes(resource.xml)/$value"
    )
    assert _get_odata_url(product_id, product_name, href) == expected_url


@pytest.mark.parametrize(
    "manifest_file_path, pattern, expected_files",
    [
        (
            "tests/download/mock/sentinel_1/manifest.safe",
            "*/calibration-*.xml",
            [
                "annotation/calibration/calibration-s1a-iw-grd-vh-20241217t061735-20241217t061800-057028-07020f-002.xml",
                "annotation/calibration/calibration-s1a-iw-grd-vv-20241217t061735-20241217t061800-057028-07020f-001.xml",
            ],
        ),
        (
            "tests/download/mock/sentinel_2/manifest.safe",
            "*TCI.jp2",
            [
                "GRANULE/L1C_T17UPV_A040535_20241209T162603/IMG_DATA/T17UPV_20241209T162609_TCI.jp2"
            ],
        ),
        (
            "tests/download/mock/sentinel_3/manifest.xml",
            "*Oa02_reflectance.nc",
            [("Oa02_reflectance.nc")],
        ),
    ],
)
def test_filter_files(manifest_file_path, pattern, expected_files):
    filtered_files = filter_files(manifest_file_path, pattern)
    assert filtered_files == expected_files


def test_filter_files_with_exclude():
    manifest_file_path = "tests/download/mock/sentinel_2/manifest.safe"
    filtered_files = filter_files(manifest_file_path, "*.jp2", exclude=True)
    assert filtered_files == [
        "MTD_MSIL1C.xml",
        "INSPIRE.xml",
        "HTML/UserProduct_index.html",
        "HTML/UserProduct_index.xsl",
        "DATASTRIP/DS_2BPS_20241209T195414_S20241209T162603/MTD_DS.xml",
        "DATASTRIP/DS_2BPS_20241209T195414_S20241209T162603/QI_DATA/FORMAT_CORRECTNESS.xml",
        "DATASTRIP/DS_2BPS_20241209T195414_S20241209T162603/QI_DATA/GENERAL_QUALITY.xml",
        "DATASTRIP/DS_2BPS_20241209T195414_S20241209T162603/QI_DATA/GEOMETRIC_QUALITY.xml",
        "DATASTRIP/DS_2BPS_20241209T195414_S20241209T162603/QI_DATA/RADIOMETRIC_QUALITY.xml",
        "DATASTRIP/DS_2BPS_20241209T195414_S20241209T162603/QI_DATA/SENSOR_QUALITY.xml",
        "GRANULE/L1C_T17UPV_A040535_20241209T162603/AUX_DATA/AUX_CAMSFO",
        "GRANULE/L1C_T17UPV_A040535_20241209T162603/AUX_DATA/AUX_ECMWFT",
        "GRANULE/L1C_T17UPV_A040535_20241209T162603/MTD_TL.xml",
        "GRANULE/L1C_T17UPV_A040535_20241209T162603/QI_DATA/FORMAT_CORRECTNESS.xml",
        "GRANULE/L1C_T17UPV_A040535_20241209T162603/QI_DATA/GENERAL_QUALITY.xml",
        "GRANULE/L1C_T17UPV_A040535_20241209T162603/QI_DATA/GEOMETRIC_QUALITY.xml",
        "GRANULE/L1C_T17UPV_A040535_20241209T162603/QI_DATA/SENSOR_QUALITY.xml",
    ]


def test_filter_files_no_match():
    manifest_file_path = "tests/download/mock/sentinel_2/manifest.safe"
    filtered_files = filter_files(manifest_file_path, "Oa1*.nc")
    assert not filtered_files


def test_download_file_success(requests_mock: Any, mocker: Any, tmp_path: Path):
    # Mock authentication
    _mock_openid(requests_mock)
    _mock_token(requests_mock)
    _mock_jwks(mocker)

    # Mock download request
    mock_url = "http://example.com/file"
    mocker.patch("cdsetool.download._follow_redirect", return_value=mock_url)
    requests_mock.get(
        mock_url,
        status_code=200,
        headers={"Content-Length": "100"},
        content=b"data" * 5,
    )

    mock_file = str(tmp_path / "mock_file")

    result = download_file(mock_url, mock_file, {})

    # Check that file was written correctly
    with open(mock_file, "rb") as f:
        file_content = f.read()
    expected_content = b"data" * 5
    assert file_content == expected_content

    assert result == mock_file


def test_download_file_failure(requests_mock: Any, mocker: Any, tmp_path: Path):
    # Mock authentication
    _mock_openid(requests_mock)
    _mock_token(requests_mock)
    _mock_jwks(mocker)

    # Mock download request
    mock_url = "http://example.com/file"
    mocker.patch("cdsetool.download._follow_redirect", return_value=mock_url)
    requests_mock.get(
        mock_url,
        status_code=404,
        headers={"Content-Length": "100"},
    )

    mock_file = str(tmp_path / "mock_file")

    # Avoid retry delay
    mocker.patch("time.sleep", return_value=None)

    result = download_file(mock_url, mock_file, {})

    assert result is None


def test_download_feature_with_filter(mocker, tmp_path):
    def mock_download_file(url, path, options):
        """Mock the download_file function to create mock files."""
        with open(path, "wb") as f:
            f.write(b"dummy data")
        return path

    options = {"filter_pattern": "*.jp2"}
    mock_feature = {
        "id": "a6215824-704b-46d7-a2ec-efea4e468668",
        "properties": {
            "title": "S2B_MSIL1C_20241209T162609_N0511_R040_T17UPV_20241209T195414.SAFE",
            "collection": "SENTINEL-2",
        },
    }
    mocker.patch(
        "cdsetool.download.filter_files",
        return_value=["./GRANULE/file1.jp2", "./GRANULE/file2.jp2"],
    )
    mocker.patch(
        "cdsetool.download.download_file",
        mock_download_file,
    )

    final_dir = tmp_path / "test_download_feature_with_filter"
    product_name = download_feature(mock_feature, final_dir, options)
    assert os.path.exists(
        os.path.join(
            final_dir,
            "S2B_MSIL1C_20241209T162609_N0511_R040_T17UPV_20241209T195414.SAFE",
            "GRANULE",
            "file1.jp2",
        )
    )
    assert os.path.exists(
        os.path.join(
            final_dir,
            "S2B_MSIL1C_20241209T162609_N0511_R040_T17UPV_20241209T195414.SAFE",
            "GRANULE",
            "file2.jp2",
        )
    )
    assert product_name == mock_feature["properties"]["title"]


def test_download_feature_with_filter_failure(mocker, tmp_path):
    options = {"filter_pattern": "*.jp2"}
    mock_feature = {
        "id": "a6215824-704b-46d7-a2ec-efea4e468668",
        "properties": {
            "title": "S2B_MSIL1C_20241209T162609_N0511_R040_T17UPV_20241209T195414.SAFE",
            "collection": "SENTINEL-2",
        },
    }
    mocker.patch(
        "cdsetool.download.filter_files",
        return_value=["./GRANULE/file1.jp2", "./GRANULE/file2.jp2"],
    )
    mocker.patch(
        "cdsetool.download.download_file",
        side_effect=lambda url, path, options: None,
    )

    final_dir = tmp_path / "test_download_feature_with_filter_failure"
    product_name = download_feature(mock_feature, final_dir, options)
    assert os.listdir(tmp_path) == []
    assert product_name is None


def test_download_feature_with_filter_unsupported_coll(caplog, tmp_path):
    options = {"logger": logging.getLogger(__name__), "filter_pattern": "*MTL.txt"}
    mock_feature = {
        "id": "a6215824-704b-46d7-a2ec-efea4e468668",
        "properties": {
            "title": "L8XXX",
            "collection": "Lansat8",
        },
    }

    final_dir = tmp_path / "test_download_feature_with_filter_unsupported_coll"
    product_name = download_feature(mock_feature, final_dir, options)
    assert os.listdir(tmp_path) == []
    assert product_name is None
    assert (
        "Downloading specific files within product bundle with node filtering"
        f" is not supported for this collection type: {mock_feature['properties']['title']}"
    ) in caplog.text
