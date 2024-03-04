"""
Logging utilities.

This module provides a NoopLogger class that outputs nothing.
"""


class NoopLogger:
    """
    A logger that does nothing.
    """

    def debug(self, *kwargs):
        """
        Log a debug message.
        """

    def info(self, *kwargs):
        """
        Log an info message.
        """

    def warning(self, *kwargs):
        """
        Log a warning message.
        """
