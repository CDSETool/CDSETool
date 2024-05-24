"""
Logging utilities.

This module provides a NoopLogger class that outputs nothing.
"""


class NoopLogger:
    """
    A logger that does nothing.
    """

    def debug(self, msg, *args, **kwargs):
        """
        Log a debug message.
        """

    def error(self, msg, *args, **kwargs):
        """
        Log an error message.
        """

    def info(self, msg, *args, **kwargs):
        """
        Log an info message.
        """

    def warning(self, msg, *args, **kwargs):
        """
        Log a warning message.
        """
