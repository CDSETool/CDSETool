import pytest
from unittest.mock import MagicMock

from cdsetool.logger import NoopLogger
from cdsetool.download import download_feature


def test_noop_logger_is_default() -> None:
    NoopLogger.debug = MagicMock()

    assert NoopLogger.debug.call_count == 0

    download_feature(
        {
            "bad_object": True,
            "properties": {
                "title": "myfile.xml",
                "services": {"download": {}},  # missing url
            },
        },
        "somewhere",
    )

    assert NoopLogger.debug.call_count == 1


def test_noop_does_not_error() -> None:
    try:
        download_feature(
            {
                "bad_object": True,
                "properties": {
                    "title": "myfile.xml",
                    "services": {"download": {}},  # missing url
                },
            },
            "somewhere",
        )
        NoopLogger.debug("NoopLogger did not raise an exception")
    except Exception as e:
        pytest.fail(f"Unexpected exception: {e}")
