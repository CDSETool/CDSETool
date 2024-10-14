"""
Logging utilities.

This module provides a NoopLogger class that outputs nothing.
"""

from typing import Any


class NoopLogger:
    """
    A logger that does nothing.
    """

    def debug(self, msg: object, *args: Any, **kwargs: Any) -> None:
        """
        Log a debug message.
        """

    def error(self, msg: object, *args: Any, **kwargs: Any) -> None:
        """
        Log an error message.
        """

    def info(self, msg: object, *args: Any, **kwargs: Any) -> None:
        """
        Log an info message.
        """

    def warning(self, msg: object, *args: Any, **kwargs: Any) -> None:
        """
        Log a warning message.
        """
