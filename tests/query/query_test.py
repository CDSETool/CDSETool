from datetime import date, datetime

import pytest

from cdsetool.query import (
    _serialize_search_term,
    _valid_match_pattern,
    _valid_max_inclusive,
    _valid_min_inclusive,
    _valid_search_term,
    geojson_to_wkt,
    shape_to_wkt,
)


def test_serialize_search_term() -> None:
    assert _serialize_search_term("foo") == "foo"
    assert _serialize_search_term(["foo", "bar"]) == "foo,bar"
    assert _serialize_search_term(date(2020, 1, 1)) == "2020-01-01"
    assert (
        _serialize_search_term(datetime(2020, 1, 1, 12, 0, 0)) == "2020-01-01T12:00:00Z"
    )


def test_validate_search_term() -> None:
    product_type = {"pattern": "^(S2MSI1C|S2MSI2A)$"}
    orbit_number = {
        "minInclusive": "1",
        "pattern": "^(\\[|\\]|[0-9])?[0-9]+$|^[0-9]+?(\\[|\\])$|^(\\[|\\])[0-9]+,[0-9]+(\\[|\\])$",
    }
    assert _valid_search_term("S2MSI1C", product_type)
    assert _valid_search_term("1", orbit_number)
    assert _valid_search_term("43212", orbit_number)

    with pytest.raises(AssertionError):
        _valid_search_term("foo", product_type)

    with pytest.raises(AssertionError):
        _valid_search_term("0", orbit_number)

    with pytest.raises(AssertionError):
        _valid_search_term("-100", orbit_number)

    with pytest.raises(AssertionError):
        _valid_search_term("foobar", orbit_number)


def test_assert_match_pattern() -> None:
    pattern = {
        "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}(T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]+)?(|Z|[\\+\\-][0-9]{2}:[0-9]{2}))?$"
    }

    with pytest.raises(AssertionError):
        _valid_match_pattern("foo", pattern)

    with pytest.raises(AssertionError):
        _valid_match_pattern("01-01-2020", pattern)

    assert _valid_match_pattern("2020-01-01", pattern)
    assert _valid_match_pattern("2020-01-01T20:31:28.888Z", pattern)

    pattern = {"pattern": "^(asc|desc|ascending|descending)$"}

    with pytest.raises(AssertionError):
        _valid_match_pattern("foo", pattern)

    with pytest.raises(AssertionError):
        _valid_match_pattern("01-01-2020", pattern)

    assert _valid_match_pattern("asc", pattern)
    assert _valid_match_pattern("descending", pattern)


def test_assert_min_inclusive() -> None:
    assert _valid_min_inclusive("1", {"minInclusive": "1"})
    assert _valid_min_inclusive("2", {"minInclusive": "1"})

    with pytest.raises(AssertionError):
        _valid_min_inclusive("0", {"minInclusive": "1"})


def test_assert_max_inclusive() -> None:
    assert _valid_max_inclusive("1", {"maxInclusive": "1"})
    assert _valid_max_inclusive("0", {"maxInclusive": "1"})

    with pytest.raises(AssertionError):
        _valid_max_inclusive("2", {"maxInclusive": "1"})


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
