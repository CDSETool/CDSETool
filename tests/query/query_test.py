"""Tests for CDSETool's query module."""

from typing import Any

from cdsetool.query import (
    _build_attribute_filter,
    _build_odata_filter,
    _fetch_collection_attributes,
    _parse_interval,
    _parse_operator_suffix,
    describe_collection,
    geojson_to_wkt,
    shape_to_wkt,
)


def test_build_attribute_filter() -> None:
    """Test OData attribute filter building"""
    result = _build_attribute_filter("productType", "S2MSI2A", "StringAttribute", "eq")
    assert "Attributes/OData.CSC.StringAttribute/any(" in result
    assert "att/Name eq 'productType'" in result
    assert "att/OData.CSC.StringAttribute/Value eq 'S2MSI2A'" in result

    result = _build_attribute_filter("cloudCover", "40.5", "DoubleAttribute", "le")
    assert "Attributes/OData.CSC.DoubleAttribute/any(" in result
    assert "att/Name eq 'cloudCover'" in result
    assert "att/OData.CSC.DoubleAttribute/Value le 40.5" in result

    result = _build_attribute_filter(
        "relativeOrbitNumber", "123", "IntegerAttribute", "eq"
    )
    assert "Attributes/OData.CSC.IntegerAttribute/any(" in result
    assert "att/Name eq 'relativeOrbitNumber'" in result
    assert "att/OData.CSC.IntegerAttribute/Value eq 123" in result

    result = _build_attribute_filter(
        "processingDate", "2024-01-15T10:30:00Z", "DateTimeOffsetAttribute", "eq"
    )
    assert "Attributes/OData.CSC.DateTimeOffsetAttribute/any(" in result
    assert "att/Name eq 'processingDate'" in result
    assert (
        "att/OData.CSC.DateTimeOffsetAttribute/Value eq 2024-01-15T10:30:00Z" in result
    )

    result = _build_attribute_filter(
        "sliceProductFlag", "true", "BooleanAttribute", "eq"
    )
    assert "Attributes/OData.CSC.BooleanAttribute/any(" in result
    assert "att/Name eq 'sliceProductFlag'" in result
    assert "att/OData.CSC.BooleanAttribute/Value eq true" in result

    result = _build_attribute_filter(
        "sliceProductFlag", "False", "BooleanAttribute", "eq"
    )
    assert "att/OData.CSC.BooleanAttribute/Value eq false" in result


def test_parse_interval() -> None:
    """Test interval syntax parsing"""
    result = _parse_interval("[10,22]")
    assert result == ("10", "22", "ge", "le")

    result = _parse_interval("(10,22)")
    assert result == ("10", "22", "gt", "lt")

    result = _parse_interval("[10,22)")
    assert result == ("10", "22", "ge", "lt")

    result = _parse_interval("(10,22]")
    assert result == ("10", "22", "gt", "le")

    result = _parse_interval("[ 10 , 22 ]")
    assert result == ("10", "22", "ge", "le")

    result = _parse_interval("[2024-01-01,2024-01-31]")
    assert result == ("2024-01-01", "2024-01-31", "ge", "le")

    assert _parse_interval("30") is None
    assert _parse_interval("abc") is None
    assert _parse_interval("[10]") is None


def test_parse_operator_suffix() -> None:
    """Test operator suffix parsing"""
    assert _parse_operator_suffix("cloudCoverLt") == ("cloudCover", "lt")
    assert _parse_operator_suffix("cloudCoverLe") == ("cloudCover", "le")
    assert _parse_operator_suffix("cloudCoverGt") == ("cloudCover", "gt")
    assert _parse_operator_suffix("cloudCoverGe") == ("cloudCover", "ge")
    assert _parse_operator_suffix("cloudCover") == ("cloudCover", "eq")
    assert _parse_operator_suffix("orbitNumberLt") == ("orbitNumber", "lt")
    assert _parse_operator_suffix("processingDateGe") == ("processingDate", "ge")


def test_build_odata_filter() -> None:
    """Test full OData filter expression building"""
    result = _build_odata_filter(
        "SENTINEL-2",
        {"contentDateStartGt": "2020-01-01", "contentDateEndLt": "2020-01-10"},
    )
    assert "Collection/Name eq 'SENTINEL-2'" in result
    assert "ContentDate/Start gt 2020-01-01" in result
    assert "ContentDate/End lt 2020-01-10" in result

    result = _build_odata_filter(
        "SENTINEL-2", {"cloudCoverEq": 40, "productType": "S2MSI2A"}
    )
    assert "Collection/Name eq 'SENTINEL-2'" in result
    assert "cloudCover" in result
    assert "Value eq 40.0" in result
    assert "productType" in result

    result = _build_odata_filter("SENTINEL-2", {"cloudCoverLe": 30})
    assert "Collection/Name eq 'SENTINEL-2'" in result
    assert "cloudCover" in result
    assert "Value le 30.0" in result

    result = _build_odata_filter("SENTINEL-2", {"cloudCover": "[10,22]"})
    assert "Collection/Name eq 'SENTINEL-2'" in result
    assert result.count("DoubleAttribute/any(") == 2
    assert "att/Name eq 'cloudCover'" in result
    assert "Value ge 10.0" in result
    assert "Value le 22.0" in result

    result = _build_odata_filter("SENTINEL-2", {"cloudCover": "(10,22)"})
    assert result.count("DoubleAttribute/any(") == 2
    assert "Value gt 10.0" in result
    assert "Value lt 22.0" in result

    result = _build_odata_filter("SENTINEL-2", {"cloudCover": "[10,22)"})
    assert result.count("DoubleAttribute/any(") == 2
    assert "Value ge 10.0" in result
    assert "Value lt 22.0" in result


def test_build_odata_filter_new_attribute_types() -> None:
    """Test OData filter building with new attribute types from ATTRIBUTES"""
    result = _build_odata_filter("SENTINEL-3", {"brightCoverEq": 50.5})
    assert "Collection/Name eq 'SENTINEL-3'" in result
    assert "Attributes/OData.CSC.DoubleAttribute/any(" in result
    assert "att/Name eq 'brightCover'" in result
    assert "att/OData.CSC.DoubleAttribute/Value eq 50.5" in result

    result = _build_odata_filter(
        "SENTINEL-1", {"processingDateEq": "2024-06-15T12:00:00Z"}
    )
    assert "Attributes/OData.CSC.DateTimeOffsetAttribute/any(" in result
    assert "att/Name eq 'processingDate'" in result
    assert "2024-06-15T12:00:00Z" in result

    result = _build_odata_filter("SENTINEL-1", {"sliceProductFlag": "true"})
    assert "Attributes/OData.CSC.BooleanAttribute/any(" in result
    assert "att/Name eq 'sliceProductFlag'" in result
    assert "att/OData.CSC.BooleanAttribute/Value eq true" in result

    result = _build_odata_filter("SENTINEL-1", {"cycleNumberEq": 42})
    assert "Attributes/OData.CSC.IntegerAttribute/any(" in result
    assert "att/Name eq 'cycleNumber'" in result
    assert "att/OData.CSC.IntegerAttribute/Value eq 42" in result

    result = _build_odata_filter("SENTINEL-1", {"timeliness": "NRT"})
    assert "Attributes/OData.CSC.StringAttribute/any(" in result
    assert "att/Name eq 'timeliness'" in result
    assert "att/OData.CSC.StringAttribute/Value eq 'NRT'" in result


def test_build_odata_filter_operator_suffixes() -> None:
    """Test OData filter building with operator suffixes"""
    result = _build_odata_filter("SENTINEL-1", {"orbitNumberLt": 100})
    assert "att/Name eq 'orbitNumber'" in result
    assert "att/OData.CSC.IntegerAttribute/Value lt 100" in result

    result = _build_odata_filter("SENTINEL-1", {"orbitNumberGe": 50})
    assert "att/Name eq 'orbitNumber'" in result
    assert "att/OData.CSC.IntegerAttribute/Value ge 50" in result

    result = _build_odata_filter("SENTINEL-2", {"cloudCoverLe": 25.5})
    assert "att/Name eq 'cloudCover'" in result
    assert "att/OData.CSC.DoubleAttribute/Value le 25.5" in result

    result = _build_odata_filter(
        "SENTINEL-1", {"processingDateGt": "2024-01-01T00:00:00Z"}
    )
    assert "att/Name eq 'processingDate'" in result
    assert (
        "att/OData.CSC.DateTimeOffsetAttribute/Value gt 2024-01-01T00:00:00Z" in result
    )


def test_build_odata_filter_date_intervals() -> None:
    """Test OData filter building with date interval syntax"""
    result = _build_odata_filter(
        "SENTINEL-2", {"contentDateStart": "[2024-01-01,2024-01-31]"}
    )
    assert "ContentDate/Start ge 2024-01-01" in result
    assert "ContentDate/Start le 2024-01-31" in result

    result = _build_odata_filter(
        "SENTINEL-2", {"contentDateEnd": "(2024-01-01,2024-01-31)"}
    )
    assert "ContentDate/End gt 2024-01-01" in result
    assert "ContentDate/End lt 2024-01-31" in result

    result = _build_odata_filter(
        "SENTINEL-1", {"processingDate": "[2024-01-01T00:00:00Z,2024-01-31T23:59:59Z]"}
    )
    assert result.count("DateTimeOffsetAttribute/any(") == 2
    assert "att/Name eq 'processingDate'" in result
    assert "Value ge 2024-01-01T00:00:00Z" in result
    assert "Value le 2024-01-31T23:59:59Z" in result


def test_shape_to_wkt() -> None:
    wkt = "POLYGON((10.172406299744779 55.48259118004532, 10.172406299744779 55.38234270718456, 10.42371976928382 55.38234270718456, 10.42371976928382 55.48259118004532, 10.172406299744779 55.48259118004532))"
    assert shape_to_wkt("tests/shape/POLYGON.shp") == wkt


def test_geojson_to_wkt() -> None:
    wkt = "POLYGON((10.172406299744779 55.48259118004532, 10.172406299744779 55.38234270718456, 10.42371976928382 55.38234270718456, 10.42371976928382 55.48259118004532, 10.172406299744779 55.48259118004532))"
    geojson = '{ "type": "Feature", "properties": { }, "geometry": { "type": "Polygon", "coordinates": [ [ [ 10.172406299744779, 55.482591180045318 ], [ 10.172406299744779, 55.382342707184563 ], [ 10.423719769283821, 55.382342707184563 ], [ 10.423719769283821, 55.482591180045318 ], [ 10.172406299744779, 55.482591180045318 ] ] ] } }'

    assert geojson_to_wkt(geojson) == wkt

    geojson = '{ "type": "Polygon", "coordinates": [ [ [ 10.172406299744779, 55.482591180045318 ], [ 10.172406299744779, 55.382342707184563 ], [ 10.423719769283821, 55.382342707184563 ], [ 10.423719769283821, 55.482591180045318 ], [ 10.172406299744779, 55.482591180045318 ] ] ] }'

    assert geojson_to_wkt(geojson) == wkt

    wkt = "POLYGON((17.58127378553624 59.88489715357605, 17.58127378553624 59.80687027682205, 17.73996723627809 59.80687027682205, 17.73996723627809 59.88489715357605, 17.58127378553624 59.88489715357605))"
    geojson = '{"type":"FeatureCollection","features":[{"type":"Feature","properties":{ },"geometry":{"coordinates":[[[17.58127378553624,59.88489715357605],[17.58127378553624,59.80687027682205],[17.73996723627809,59.80687027682205],[17.73996723627809,59.88489715357605],[17.58127378553624,59.88489715357605]]],"type":"Polygon" } } ] }'

    assert geojson_to_wkt(geojson) == wkt


def _mock_sentinel_1_attributes(requests_mock: Any) -> None:
    """Mock the Attributes endpoint for SENTINEL-1"""
    with open(
        "tests/query/mock/sentinel_1/attributes.json", "r", encoding="utf-8"
    ) as f:
        requests_mock.get(
            "https://catalogue.dataspace.copernicus.eu/odata/v1/Attributes(SENTINEL-1)",
            text=f.read(),
        )


def test_fetch_collection_attributes(requests_mock: Any) -> None:
    """Test fetching attributes from OData API"""
    _mock_sentinel_1_attributes(requests_mock)

    attrs = _fetch_collection_attributes("SENTINEL-1")

    assert attrs is not None
    attr_names = [a["Name"] for a in attrs]
    assert "productType" in attr_names
    assert "orbitNumber" in attr_names
    assert "relativeOrbitNumber" in attr_names

    product_type = next(a for a in attrs if a["Name"] == "productType")
    assert product_type["ValueType"] == "String"

    orbit_number = next(a for a in attrs if a["Name"] == "orbitNumber")
    assert orbit_number["ValueType"] == "Integer"


def test_fetch_collection_attributes_not_found(requests_mock: Any) -> None:
    """Test fetching attributes for non-existent collection"""
    requests_mock.get(
        "https://catalogue.dataspace.copernicus.eu/odata/v1/Attributes(INVALID)",
        status_code=400,
    )
    assert _fetch_collection_attributes("INVALID") is None


def test_describe_collection_returns_all_server_attrs_as_supported(
    requests_mock: Any,
) -> None:
    """Test that describe_collection returns all server attributes as supported"""
    _mock_sentinel_1_attributes(requests_mock)

    result = describe_collection("SENTINEL-1")

    assert result is not None
    assert "contentDateStart" in result
    assert "contentDateEnd" in result
    assert "geometry" in result
    assert "productType" in result
    assert "orbitDirection" in result
    assert "relativeOrbitNumber" in result
    assert "orbitNumber" in result
    assert "datatakeID" in result
    assert "cycleNumber" in result
    assert "sliceNumber" in result
    assert "processorName" in result
    assert "processingDate" in result
    assert "sliceProductFlag" in result
    assert "startTimeFromAscendingNode" in result
