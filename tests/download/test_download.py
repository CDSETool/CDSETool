"""Tests for CDSETool's download module."""

import logging
import os
from unittest import mock

from cdsetool.download import _href_to_url, download_file, download_nodes, filter_files


def test_href_to_url():
    odata_url = "http://example.com/odata/v1"
    product_id = "a6215824-704b-46d7-a2ec-efea4e468668"
    product_name = "S2B_MSIL1C_20241209T162609_N0511_R040_T17UPV_20241209T195414.SAFE"
    href = "path/to/resource.xml"
    expected_url = (
        "http://example.com/odata/v1/Products(a6215824-704b-46d7-a2ec-efea4e468668)/"
        "Nodes(S2B_MSIL1C_20241209T162609_N0511_R040_T17UPV_20241209T195414.SAFE)/"
        "Nodes(path)/Nodes(to)/Nodes(resource.xml)/$value"
    )
    assert _href_to_url(odata_url, product_id, product_name, href) == expected_url


def test_filter_files_s1():
    manifest_file_path = "tests/download/mock/sentinel_1/manifest.safe"
    filtered_files = filter_files(manifest_file_path, "*/calibration-*.xml")
    filtered_files.sort()
    assert filtered_files == [
        (
            "annotation/calibration/"
            "calibration-s1a-iw-grd-vh-20241217t061735-20241217t061800-057028-07020f-002.xml"
        ),
        (
            "annotation/calibration/"
            "calibration-s1a-iw-grd-vv-20241217t061735-20241217t061800-057028-07020f-001.xml"
        ),
    ]


def test_filter_files_s2():
    manifest_file_path = "tests/download/mock/sentinel_2/manifest.safe"
    filtered_files = filter_files(manifest_file_path, "*TCI.jp2")
    assert filtered_files == [
        (
            "GRANULE/L1C_T17UPV_A040535_20241209T162603/"
            "IMG_DATA/T17UPV_20241209T162609_TCI.jp2"
        )
    ]


def test_filter_files_s3():
    manifest_file_path = "tests/download/mock/sentinel_3/manifest.xml"
    filtered_files = filter_files(manifest_file_path, "*Oa02_reflectance.nc")
    assert filtered_files == [("Oa02_reflectance.nc")]


def test_filter_files_with_exclude():
    manifest_file_path = "tests/download/mock/sentinel_2/manifest.safe"
    filtered_files = filter_files(manifest_file_path, "*.jp2", exclude=True)
    filtered_files.sort()
    assert filtered_files == [
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
        "HTML/UserProduct_index.html",
        "HTML/UserProduct_index.xsl",
        "INSPIRE.xml",
        "MTD_MSIL1C.xml",
    ]


def test_filter_files_no_match():
    manifest_file_path = "tests/download/mock/sentinel_2/manifest.safe"
    filtered_files = filter_files(manifest_file_path, "Oa1*.nc")
    assert not filtered_files


def test_download_file_success(mocker, tmp_path):
    url = "http://example.com/file"
    options = {}

    mock_response = mock.MagicMock()
    mock_response.status_code = 200
    mock_response.headers = {"Content-Length": "100"}
    mock_response.iter_content = lambda chunk_size: [b"data"] * 5

    mock_session = mock.MagicMock
    mock_get = mock.MagicMock()
    mock_get.return_value.__enter__.return_value = mock_response
    mock_session.get = mock_get

    mock_credentials = mock.MagicMock()
    mock_credentials.get_session = mock.MagicMock(return_value=mock_session)
    mocker.patch("cdsetool.download._get_credentials", return_value=mock_credentials)
    mocker.patch("cdsetool.download._follow_redirect", return_value=url)

    mock_file = tmp_path / "mock_file"

    result = download_file(url, mock_file, options)

    # Check that file was written correctly
    with open(mock_file, "rb") as f:
        file_content = f.read()
    expected_content = b"data" * 5
    assert file_content == expected_content

    assert result == mock_file


def test_download_file_failure(mocker, tmp_path):
    url = "http://example.com/file"
    options = {}

    mock_response = mock.MagicMock()
    mock_response.status_code = 404
    mock_response.headers = {"Content-Length": "100"}

    mock_session = mock.MagicMock
    mock_get = mock.MagicMock()
    mock_get.return_value.__enter__.return_value = mock_response
    mock_session.get = mock_get

    mock_credentials = mock.MagicMock()
    mock_credentials.get_session = mock.MagicMock(return_value=mock_session)
    mocker.patch("cdsetool.download._get_credentials", return_value=mock_credentials)
    mocker.patch("cdsetool.download._follow_redirect", return_value=url)
    mocker.patch("time.sleep", return_value=None)

    mock_file = tmp_path / "mock_file"

    result = download_file(url, mock_file, options)

    assert result is None


def test_download_nodes_success(mocker, tmp_path):
    def mock_download_file(url, path, options):
        """Mock the download_file function to create mock files."""
        with open(path, "wb") as f:
            f.write(b"dummy data")
        return path

    options = {}
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

    final_dir = tmp_path / "test_download_nodes_success"
    product_name = download_nodes(mock_feature, final_dir, "*.jp2", options)
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


def test_download_nodes_failure(mocker, tmp_path):
    options = {}
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

    final_dir = tmp_path / "test_download_nodes_failure"
    product_name = download_nodes(mock_feature, final_dir, "*.jp2", options)
    assert os.listdir(tmp_path) == []
    assert product_name is None


def test_download_nodes_unsupported_coll(caplog, tmp_path):
    options = {"logger": logging.getLogger(__name__)}
    mock_feature = {
        "id": "a6215824-704b-46d7-a2ec-efea4e468668",
        "properties": {
            "title": "L8XXX",
            "collection": "Lansat8",
        },
    }

    final_dir = tmp_path / "test_download_nodes_unsupported_coll"
    product_name = download_nodes(mock_feature, final_dir, "*MTL.txt", options)
    assert os.listdir(tmp_path) == []
    assert product_name is None
    assert (
        "Downloading specific files within product bundle with node filtering"
        f" is not supported for this collection type: {mock_feature['properties']['title']}"
    ) in caplog.text
