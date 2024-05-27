"""
Download result data class
"""

from typing import Any, Dict, Optional
from dataclasses import dataclass


@dataclass
class DownloadResult:
    """
    Describes the result of a download operation, whether it was successful or not.
    Contains relevant information about the feature and the download operation.
    """

    success: bool
    feature: Dict[str, Any]
    filename: Optional[str]
    message: Optional[str]

    @staticmethod
    def ok(feature, filename):
        """
        Create a successful DownloadResult
        """
        return DownloadResult(True, feature, filename, None)

    @staticmethod
    def fail(feature, message):
        """
        Create a failed DownloadResult
        """
        return DownloadResult(False, feature, None, message)

    def __str__(self):
        if self.success:
            return f"Downloaded {self.feature.get('id')} to {self.filename}"

        return f"Failed to download {self.feature.get('id')}: {self.message}"
