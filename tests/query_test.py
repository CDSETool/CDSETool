import pytest

from cdsetool.query import _serialize_search_term
from datetime import date, datetime


def test_serialize_search_term():
    assert _serialize_search_term("foo") == "foo"
    assert _serialize_search_term(["foo", "bar"]) == "foo,bar"
    assert _serialize_search_term(date(2020, 1, 1)) == "2020-01-01"
    assert (
        _serialize_search_term(datetime(2020, 1, 1, 12, 0, 0)) == "2020-01-01T12:00:00Z"
    )
