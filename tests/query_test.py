import pytest

from cdsetool.query import (
    _serialize_search_term,
    _validate_search_term,
    _assert_match_pattern,
    _assert_valid_key,
    _assert_min_inclusive,
    _assert_max_inclusive,
)
from datetime import date, datetime


def test_serialize_search_term():
    assert _serialize_search_term("foo") == "foo"
    assert _serialize_search_term(["foo", "bar"]) == "foo,bar"
    assert _serialize_search_term(date(2020, 1, 1)) == "2020-01-01"
    assert (
        _serialize_search_term(datetime(2020, 1, 1, 12, 0, 0)) == "2020-01-01T12:00:00Z"
    )


def test_validate_search_term():
    description = {
        "productType": {"pattern": "^(S2MSI1C|S2MSI2A)$"},
        "orbitNumber": {
            "minInclusive": "1",
            "pattern": "^(\\[|\\]|[0-9])?[0-9]+$|^[0-9]+?(\\[|\\])$|^(\\[|\\])[0-9]+,[0-9]+(\\[|\\])$",
        },
    }
    _validate_search_term("productType", "S2MSI1C", description)
    _validate_search_term("orbitNumber", "1", description)
    _validate_search_term("orbitNumber", "43212", description)

    with pytest.raises(AssertionError):
        _validate_search_term("productType", "foo", description)

    with pytest.raises(AssertionError):
        _validate_search_term("orbitNumber", "0", description)

    with pytest.raises(AssertionError):
        _validate_search_term("orbitNumber", "-100", description)

    with pytest.raises(AssertionError):
        _validate_search_term("orbitNumber", "foobar", description)


def test_assert_valid_key():
    _assert_valid_key("someKey", {"someKey": True})

    with pytest.raises(AssertionError):
        _assert_valid_key("otherKey", {"someKey": True})


def test_assert_match_pattern():
    pattern = "^[0-9]{4}-[0-9]{2}-[0-9]{2}(T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]+)?(|Z|[\\+\\-][0-9]{2}:[0-9]{2}))?$"

    with pytest.raises(AssertionError):
        _assert_match_pattern("foo", pattern)

    with pytest.raises(AssertionError):
        _assert_match_pattern("01-01-2020", pattern)

    _assert_match_pattern("2020-01-01", None)
    _assert_match_pattern("2020-01-01", pattern)
    _assert_match_pattern("2020-01-01T20:31:28.888Z", pattern)

    pattern = "^(asc|desc|ascending|descending)$"

    with pytest.raises(AssertionError):
        _assert_match_pattern("foo", pattern)

    with pytest.raises(AssertionError):
        _assert_match_pattern("01-01-2020", pattern)

    _assert_match_pattern("asc", pattern)
    _assert_match_pattern("descending", pattern)


def test_assert_min_inclusive():
    _assert_min_inclusive(1, 1)
    _assert_min_inclusive(2, 1)

    with pytest.raises(AssertionError):
        _assert_min_inclusive(0, 1)


def test_assert_max_inclusive():
    _assert_max_inclusive(1, 1)
    _assert_max_inclusive(0, 1)

    with pytest.raises(AssertionError):
        _assert_max_inclusive(2, 1)
