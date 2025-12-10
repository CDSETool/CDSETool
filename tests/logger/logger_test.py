"""Tests for CDSETool's logger module."""

from unittest.mock import MagicMock

import pytest

from cdsetool.download import download_feature
from cdsetool.logger import NoopLogger


def test_noop_logger_is_default() -> None:
    NoopLogger.debug = MagicMock()

    assert NoopLogger.debug.call_count == 0

    download_feature(
        {
            "bad_object": True,
            "Name": "myfile.xml",
            # Missing Id will cause bad URL
        },
        "somewhere",
    )

    assert NoopLogger.debug.call_count == 1


def test_noop_does_not_error() -> None:
    try:
        download_feature(
            {
                "bad_object": True,
                "Name": "myfile.xml",
                # Missing Id will cause bad URL
            },
            "somewhere",
        )
        NoopLogger().debug("NoopLogger did not raise an exception")
    except Exception as e:
        pytest.fail(f"Unexpected exception: {e}")
