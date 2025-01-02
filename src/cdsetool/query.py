"""
Query the Copernicus Data Space Ecosystem OpenSearch API

https://documentation.dataspace.copernicus.eu/APIs/OpenSearch.html
"""

import json
import re
from datetime import date, datetime
from random import random
from time import sleep
from typing import Any, Dict, Union
from xml.etree import ElementTree

import geopandas as gpd
from requests.exceptions import ChunkedEncodingError
from urllib3.exceptions import ProtocolError

from cdsetool.credentials import Credentials
from cdsetool.logger import NoopLogger


class _FeatureIterator:
    def __init__(self, feature_query) -> None:
        self.index = 0
        self.feature_query = feature_query

    def __len__(self) -> int:
        return len(self.feature_query)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            item = self.feature_query[self.index]
            self.index += 1
            return item
        except IndexError as exc:
            raise StopIteration from exc


class FeatureQuery:
    """
    An iterator over the features matching the search terms

    Queries the API in batches (default: 50) features, and returns them one by one.
    Queries the next batch when the current batch is exhausted.
    """

    total_results: int = -1

    def __init__(
        self,
        collection: str,
        search_terms: Dict[str, Any],
        proxies: Union[Dict[str, str], None] = None,
        options: Union[Dict[str, Any], None] = None,
    ) -> None:
        self.features = []
        self.proxies = proxies
        self.log = (options or {}).get("logger") or NoopLogger()
        self.next_url = _query_url(
            collection,
            {**search_terms, "exactCount": "1"},
            proxies=proxies,
            validate_search_terms=(options or {}).get("validate_search_terms", True),
        )

    def __iter__(self):
        return _FeatureIterator(self)

    def __len__(self) -> int:
        if self.total_results < 0:
            self.__fetch_features()

        return self.total_results

    def __getitem__(self, index: int):
        while index >= len(self.features) and self.next_url is not None:
            self.__fetch_features()

        return self.features[index]

    def __fetch_features(self) -> None:
        if self.next_url is None:
            return
        session = Credentials.make_session(
            None, False, Credentials.RETRIES, self.proxies
        )
        attempts = 0
        while attempts < 10:
            attempts += 1
            try:
                with session.get(self.next_url) as response:
                    if response.status_code != 200:
                        self.log.warning(
                            f"Status code {response.status_code}, retrying.."
                        )
                        sleep(60 * (1 + (random() / 4)))
                        continue
                    res = response.json()
                    self.features += res.get("features") or []

                    total_results = res.get("properties", {}).get("totalResults")
                    if total_results is not None:
                        self.total_results = total_results

                    self.__set_next_url(res)
                    return
            except (ChunkedEncodingError, ConnectionResetError, ProtocolError) as e:
                self.log.warning(e)
                continue

    def __set_next_url(self, res) -> None:
        links = res.get("properties", {}).get("links") or []
        self.next_url = next(
            (link for link in links if link.get("rel") == "next"), {}
        ).get("href")

        if self.next_url:
            self.next_url = self.next_url.replace("exactCount=1", "exactCount=0")


def query_features(
    collection: str,
    search_terms: Dict[str, Any],
    proxies: Union[Dict[str, str], None] = None,
    options: Union[Dict[str, Any], None] = None,
) -> FeatureQuery:
    """
    Returns an iterator over the features matching the search terms
    """
    return FeatureQuery(
        collection, {"maxRecords": 2000, **search_terms}, proxies, options
    )


def shape_to_wkt(shape: str) -> str:
    """
    Convert a shapefile to a WKT string
    """
    coordinates = list(gpd.read_file(shape).geometry[0].exterior.coords)
    return (
        "POLYGON(("
        + ", ".join(" ".join(map(str, coord)) for coord in coordinates)
        + "))"
    )


def geojson_to_wkt(geojson_in: Union[str, Dict]) -> str:
    """
    Convert a geojson geometry to a WKT string
    """
    geojson = json.loads(geojson_in) if isinstance(geojson_in, str) else geojson_in

    if geojson.get("type") == "Feature":
        geojson = geojson["geometry"]
    elif geojson.get("type") == "FeatureCollection" and len(geojson["features"]) == 1:
        geojson = geojson["features"][0]["geometry"]

    coordinates = str(
        tuple(item for sublist in geojson["coordinates"][0] for item in sublist)
    )
    paired_coord = ",".join(
        [
            f"{a}{b}"
            for a, b in zip(coordinates.split(",")[0::2], coordinates.split(",")[1::2])
        ]
    )
    return f"POLYGON({paired_coord})"


def describe_collection(
    collection: str, proxies: Union[Dict[str, str], None] = None
) -> Dict[str, Any]:
    """
    Get a list of valid options for a given collection in key value pairs
    """
    content = _get_describe_doc(collection, proxies=proxies)
    tree = ElementTree.fromstring(content)
    parameter_node_parent = tree.find(
        "{http://a9.com/-/spec/opensearch/1.1/}Url[@type='application/json']"
    )

    parameters = {}
    if parameter_node_parent is None:
        return parameters
    for parameter_node in parameter_node_parent:
        name = parameter_node.attrib.get("name")
        pattern = parameter_node.attrib.get("pattern")
        min_inclusive = parameter_node.attrib.get("minInclusive")
        max_inclusive = parameter_node.attrib.get("maxInclusive")
        title = parameter_node.attrib.get("title")

        if name:
            parameters[name] = {
                "pattern": pattern,
                "minInclusive": min_inclusive,
                "maxInclusive": max_inclusive,
                "title": title,
            }

    return parameters


def _query_url(
    collection: str,
    search_terms: Dict[str, Any],
    proxies: Union[Dict[str, str], None],
    validate_search_terms: bool,
) -> str:
    description = (
        describe_collection(collection, proxies=proxies)
        if validate_search_terms
        else {}
    )
    query_list = []
    for key, value in search_terms.items():
        val = _serialize_search_term(value)
        valid = True
        if validate_search_terms:
            cfg = description.get(key)
            if cfg is None:
                assert False, (
                    f'search_term with name "{key}" was not found for collection.'
                    + f" Available terms are: {', '.join(description.keys())}"
                )
                continue
            valid = _valid_search_term(val, cfg)
        if valid:
            query_list.append(f"{key}={val}")

    return (
        "https://catalogue.dataspace.copernicus.eu"
        + f"/resto/api/collections/{collection}/search.json?{'&'.join(query_list)}"
    )


def _serialize_search_term(search_term: object) -> str:
    if isinstance(search_term, list):
        return ",".join(search_term)

    if isinstance(search_term, datetime):
        return search_term.strftime("%Y-%m-%dT%H:%M:%SZ")

    if isinstance(search_term, date):
        return search_term.strftime("%Y-%m-%d")

    return str(search_term)


def _valid_search_term(search_term: str, cfg: Dict[str, str]) -> bool:
    return (
        _valid_match_pattern(search_term, cfg)
        and _valid_min_inclusive(search_term, cfg)
        and _valid_max_inclusive(search_term, cfg)
    )


def _valid_match_pattern(search_term: str, cfg: Dict[str, str]) -> bool:
    pattern = cfg.get("pattern")
    if not pattern:
        return True

    if re.match(pattern, search_term) is None:
        assert False, f"search_term {search_term} does not match pattern {pattern}"
        return False
    return True


def _valid_min_inclusive(search_term: str, cfg: Dict[str, str]) -> bool:
    min_inclusive = cfg.get("minInclusive")
    if not min_inclusive:
        return True

    if int(search_term) < int(min_inclusive):
        assert (
            False
        ), f"search_term {search_term} is less than min_inclusive {min_inclusive}"
        return False
    return True


def _valid_max_inclusive(search_term: str, cfg: Dict[str, str]) -> bool:
    max_inclusive = cfg.get("maxInclusive")
    if not max_inclusive:
        return True

    if int(search_term) > int(max_inclusive):
        assert (
            False
        ), f"search_term {search_term} is greater than max_inclusive {max_inclusive}"
        return False
    return True


_describe_docs: Dict[str, bytes] = {}


def _get_describe_doc(
    collection: str, proxies: Union[Dict[str, str], None] = None
) -> bytes:
    docs = _describe_docs.get(collection)
    if docs:
        return docs
    session = Credentials.make_session(None, False, Credentials.RETRIES, proxies)
    attempts = 0
    while attempts < 10:
        attempts += 1
        with session.get(
            "https://catalogue.dataspace.copernicus.eu"
            f"/resto/api/collections/{collection}/describe.xml"
        ) as res:
            if res.status_code >= 500:
                sleep(60 * (1 + (random() / 4)))
                continue
            assert res.status_code == 200, (
                f"Unable to find collection with name {collection}. Please see "
                "https://documentation.dataspace.copernicus.eu"
                "/APIs/OpenSearch.html#collections for a list of collections"
            )

            _describe_docs[collection] = res.content
            return res.content
    assert False, f"Failed {attempts} times to get collection {collection}, giving up."


def get_odata_by_name(name: str, proxies: Union[Dict[str, str], None] = None) -> Dict:
    """Get odata for checksum given a product name"""
    session = Credentials.make_session(None, False, Credentials.RETRIES, proxies)
    attempts = 0
    while attempts < 10:
        attempts += 1
        with session.get(
            "https://catalogue.dataspace.copernicus.eu"
            f"/odata/v1/Products?$filter=Name eq '{name}'"
        ) as res:
            if res.status_code != 200:
                sleep(60 * (1 + (random() / 4)))
                continue
            assert res.status_code == 200, (
                f"Unable to find product checksum with name {name}. Please see "
                "https://documentation.dataspace.copernicus.eu"
                "/APIs/OData.html#query-by-name"
            )
            return res.json()  # type: ignore
    assert False, f"Failed {attempts} times to get product checksum {name}, giving up."
