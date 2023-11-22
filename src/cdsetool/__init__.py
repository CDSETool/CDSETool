from cdsetool.query import query_features
from cdsetool.credentials import Credentials
from cdsetool.download import download_features
from cdsetool.monitor import StatusMonitor


class CDSETool:
    """
    A class for querying and downloading data from the Copernicus Data Space Ecosystem
    """

    def __init__(self, **options):
        self.options = options

    def query(self, collection, search_terms=None):
        """
        Query the Copernicus Data Space Ecosystem OpenSearch API
        """
        search_terms = search_terms or {}
        return query_features(collection, search_terms)

    def download(self, downloadable, path):
        """
        Download features from a Copernicus Data Space Ecosystem OpenSearch API result
        """
        options = {"credentials": self._credentials()}
        if self.options.get("progress", True):
            options["monitor"] = StatusMonitor
        return list(download_features(downloadable, path, options=options))

    def _credentials(self):
        return Credentials(
            self.options.get("username"),
            self.options.get("password"),
            self.options.get("token_endpoint"),
        )
