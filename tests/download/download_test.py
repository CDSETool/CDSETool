"""Tests for CDSETool's download module."""

import logging
import os
from pathlib import Path
from typing import Any, Dict, List

import pytest

from cdsetool.credentials import Credentials
from cdsetool.download import (
    _get_feature_url,
    _get_odata_url,
    download_feature,
    download_file,
    filter_files,
)

from ..mock_auth import mock_jwks, mock_openid, mock_token


def mock_download_file(url: str, path: str, options: Dict[str, Any]) -> bool:
    """Mock the download_file function to create mock files."""
    with open(path, "wb") as f:
        f.write(b"dummy data")
    return True


def test_get_feature_url() -> None:
    """Test full product OData download URL generation."""
    feature = {"Id": "a6215824-704b-46d7-a2ec-efea4e468668"}
    expected_url = (
        "https://download.dataspace.copernicus.eu/odata/v1/"
        "Products(a6215824-704b-46d7-a2ec-efea4e468668)/$value"
    )
    assert _get_feature_url(feature) == expected_url
    assert _get_feature_url({}) == ""
    assert _get_feature_url({"Id": None}) == ""


def test_get_odata_url() -> None:
    """Test individual file OData download URL generation."""
    product_id = "a6215824-704b-46d7-a2ec-efea4e468668"
    product_name = "S2B_MSIL1C_20241209T162609_N0511_R040_T17UPV_20241209T195414.SAFE"
    href = "path/to/resource.xml"
    expected_url = (
        "https://download.dataspace.copernicus.eu/odata/v1/"
        "Products(a6215824-704b-46d7-a2ec-efea4e468668)/"
        f"Nodes({product_name})/"
        "Nodes(path)/Nodes(to)/Nodes(resource.xml)/$value"
    )
    assert _get_odata_url(product_id, product_name, href) == expected_url


@pytest.mark.parametrize(
    "manifest_file_path, pattern, expected_files",
    [
        (
            Path("tests/download/mock/sentinel_1/manifest.safe"),
            "*/calibration-*.xml",
            [
                Path(
                    "annotation/calibration/calibration-s1a-iw-grd-vh-20241217t061735-20241217t061800-057028-07020f-002.xml"
                ),
                Path(
                    "annotation/calibration/calibration-s1a-iw-grd-vv-20241217t061735-20241217t061800-057028-07020f-001.xml"
                ),
            ],
        ),
        (
            Path("tests/download/mock/sentinel_2/manifest.safe"),
            "*TCI.jp2",
            [
                Path(
                    "GRANULE/L1C_T17UPV_A040535_20241209T162603/IMG_DATA/T17UPV_20241209T162609_TCI.jp2"
                )
            ],
        ),
        (
            Path("tests/download/mock/sentinel_3/xfdumanifest.xml"),
            "*Oa02_reflectance.nc",
            [Path("Oa02_reflectance.nc")],
        ),
    ],
)
def test_filter_files(
    manifest_file_path: Path, pattern: str, expected_files: List[str]
) -> None:
    filtered_files = filter_files(manifest_file_path, pattern)
    assert filtered_files == expected_files


def test_filter_files_with_exclude() -> None:
    manifest_file_path = Path("tests/download/mock/sentinel_2/manifest.safe")
    filtered_files = filter_files(manifest_file_path, "*.jp2", exclude=True)
    assert filtered_files == [
        Path("MTD_MSIL1C.xml"),
        Path("INSPIRE.xml"),
        Path("HTML/UserProduct_index.html"),
        Path("HTML/UserProduct_index.xsl"),
        Path("DATASTRIP/DS_2BPS_20241209T195414_S20241209T162603/MTD_DS.xml"),
        Path(
            "DATASTRIP/DS_2BPS_20241209T195414_S20241209T162603/QI_DATA/FORMAT_CORRECTNESS.xml"
        ),
        Path(
            "DATASTRIP/DS_2BPS_20241209T195414_S20241209T162603/QI_DATA/GENERAL_QUALITY.xml"
        ),
        Path(
            "DATASTRIP/DS_2BPS_20241209T195414_S20241209T162603/QI_DATA/GEOMETRIC_QUALITY.xml"
        ),
        Path(
            "DATASTRIP/DS_2BPS_20241209T195414_S20241209T162603/QI_DATA/RADIOMETRIC_QUALITY.xml"
        ),
        Path(
            "DATASTRIP/DS_2BPS_20241209T195414_S20241209T162603/QI_DATA/SENSOR_QUALITY.xml"
        ),
        Path("GRANULE/L1C_T17UPV_A040535_20241209T162603/AUX_DATA/AUX_CAMSFO"),
        Path("GRANULE/L1C_T17UPV_A040535_20241209T162603/AUX_DATA/AUX_ECMWFT"),
        Path("GRANULE/L1C_T17UPV_A040535_20241209T162603/MTD_TL.xml"),
        Path(
            "GRANULE/L1C_T17UPV_A040535_20241209T162603/QI_DATA/FORMAT_CORRECTNESS.xml"
        ),
        Path("GRANULE/L1C_T17UPV_A040535_20241209T162603/QI_DATA/GENERAL_QUALITY.xml"),
        Path(
            "GRANULE/L1C_T17UPV_A040535_20241209T162603/QI_DATA/GEOMETRIC_QUALITY.xml"
        ),
        Path("GRANULE/L1C_T17UPV_A040535_20241209T162603/QI_DATA/SENSOR_QUALITY.xml"),
    ]


def test_filter_files_no_match() -> None:
    manifest_file_path = Path("tests/download/mock/sentinel_2/manifest.safe")
    filtered_files = filter_files(manifest_file_path, "Oa1*.nc")
    assert not filtered_files


def test_filter_files_broken_manifest() -> None:
    manifest_file_path = Path("tests/download/mock/sentinel_2/broken_manifest.safe")
    filtered_files = filter_files(manifest_file_path, "*TCI.jp2")
    assert filtered_files is None


def test_download_file_success(requests_mock: Any, mocker: Any, tmp_path: Path) -> None:
    mock_openid(requests_mock)
    mock_token(requests_mock)
    mock_jwks(mocker)

    mock_url = "http://example.com/file"
    mocker.patch("cdsetool.download._follow_redirect", return_value=mock_url)
    content = b"data" * 5
    requests_mock.get(
        mock_url, status_code=200, headers={"Content-Length": "100"}, content=content
    )
    mock_file = tmp_path / "mock_file"

    result = download_file(
        mock_url, mock_file, {"credentials": Credentials("usr", "pwd")}
    )

    assert result is True

    # Check that file was written correctly
    with open(mock_file, "rb") as f:
        file_content = f.read()
    assert file_content == content


def test_download_file_failure(requests_mock: Any, mocker: Any, tmp_path: Path) -> None:
    mock_openid(requests_mock)
    mock_token(requests_mock)
    mock_jwks(mocker)

    mock_url = "http://example.com/file"
    mocker.patch("cdsetool.download._follow_redirect", return_value=mock_url)
    requests_mock.get(mock_url, status_code=404, headers={"Content-Length": "100"})
    mock_file = tmp_path / "mock_file"
    mocker.patch("time.sleep", return_value=None)  # Avoid retry delay

    result = download_file(
        mock_url, mock_file, {"credentials": Credentials("usr", "pwd")}
    )
    assert result is False


def test_download_feature(mocker: Any, tmp_path: Path) -> None:
    title = "S2B_MSIL1C_20241209T162609_N0511_R040_T17UPV_20241209T195414.SAFE"
    mock_feature = {
        "Id": "a6215824-704b-46d7-a2ec-efea4e468668",
        "Name": title,
        "ContentLength": 1000,
        "Online": True,
    }
    mocker.patch("cdsetool.download.download_file", mock_download_file)

    final_dir = str(tmp_path / "test_download_feature")
    filename = download_feature(mock_feature, final_dir)
    assert filename == f"{title}.zip"
    assert os.path.exists(os.path.join(final_dir, f"{title}.zip"))


def test_download_feature_failure(mocker: Any, tmp_path: Path) -> None:
    title = "S2B_MSIL1C_20241209T162609_N0511_R040_T17UPV_20241209T195414.SAFE"
    mock_feature = {
        "Id": "a6215824-704b-46d7-a2ec-efea4e468668",
        "Name": title,
        "ContentLength": 1000,
        "Online": True,
    }
    mocker.patch(
        "cdsetool.download.download_file", side_effect=lambda url, path, options: None
    )

    final_dir = str(tmp_path / "test_download_feature_failure")
    filename = download_feature(mock_feature, final_dir)
    assert filename is None


def test_download_feature_with_filter(mocker: Any, tmp_path: Path) -> None:
    options = {"filter_pattern": "*.jp2"}
    title = "S2B_MSIL1C_20241209T162609_N0511_R040_T17UPV_20241209T195414.SAFE"
    mock_feature = {
        "Id": "a6215824-704b-46d7-a2ec-efea4e468668",
        "Name": title,
        "ContentLength": 1000,
        "Online": True,
        "Collection": "SENTINEL-2",
    }
    mocker.patch(
        "cdsetool.download.filter_files",
        return_value=[Path("./GRANULE/file1.jp2"), Path("./GRANULE/file2.jp2")],
    )
    mocker.patch("cdsetool.download.download_file", mock_download_file)

    final_dir = str(tmp_path / "test_download_feature_with_filter")
    product_name = download_feature(mock_feature, final_dir, options)
    assert product_name == mock_feature["Name"]
    assert os.path.exists(os.path.join(final_dir, title, "GRANULE", "file1.jp2"))
    assert os.path.exists(os.path.join(final_dir, title, "GRANULE", "file2.jp2"))


def test_download_feature_with_filter_failure(mocker: Any, tmp_path: Path) -> None:
    options = {"filter_pattern": "*.jp2"}
    mock_feature = {
        "Id": "a6215824-704b-46d7-a2ec-efea4e468668",
        "Name": "S2B_MSIL1C_20241209T162609_N0511_R040_T17UPV_20241209T195414.SAFE",
        "ContentLength": 1000,
        "Online": True,
        "Collection": "SENTINEL-2",
    }
    mocker.patch(
        "cdsetool.download.filter_files",
        return_value=["./GRANULE/file1.jp2", "./GRANULE/file2.jp2"],
    )
    mocker.patch(
        "cdsetool.download.download_file", side_effect=lambda url, path, options: None
    )

    final_dir = str(tmp_path / "test_download_feature_with_filter_failure")
    product_name = download_feature(mock_feature, final_dir, options)
    assert product_name is None


def test_download_feature_with_filter_unsupported_coll(
    caplog: Any, tmp_path: Path
) -> None:
    options = {"logger": logging.getLogger(__name__), "filter_pattern": "*MTL.txt"}
    mock_feature = {
        "Id": "a6215824-704b-46d7-a2ec-efea4e468668",
        "Name": "L8XXX",
        "ContentLength": 1000,
        "Online": True,
        "Collection": "Landsat8",
    }

    final_dir = str(tmp_path / "test_download_feature_with_filter_unsupported_coll")
    product_name = download_feature(mock_feature, final_dir, options)
    assert product_name is None
    assert (
        "No support for downloading individual files in "
        f"{mock_feature['Collection']} products"
    ) in caplog.text
