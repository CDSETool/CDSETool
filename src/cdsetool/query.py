"""
Query the Copernicus Data Space Ecosystem

https://documentation.dataspace.copernicus.eu/APIs/OData.html
"""

import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from random import random
from time import sleep
from typing import Any, Dict, Final, List, Literal, Optional, TypeVar, Union
from urllib.parse import quote

import geopandas as gpd
from requests.exceptions import ChunkedEncodingError
from urllib3.exceptions import ProtocolError

from cdsetool._attributes import ATTRIBUTES
from cdsetool.credentials import Credentials
from cdsetool.logger import NoopLogger

SearchTermValue = Union[str, int, float, bool, date, datetime]

ODataComparisonOp = Literal["eq", "lt", "le", "gt", "ge"]

ODataAttributeType = Literal[
    "StringAttribute",
    "IntegerAttribute",
    "DoubleAttribute",
    "DateTimeOffsetAttribute",
    "BooleanAttribute",
]

T = TypeVar("T")

# API-imposed limits from the Copernicus OData API
MAX_BATCH_SIZE: Final = 1000


@dataclass(frozen=True)
class DateFilterSpec:
    """Specification for a date-based filter."""

    odata_field: str
    operator: ODataComparisonOp
    title: str
    interval_only: bool


_OPERATOR_LABELS: Dict[ODataComparisonOp, str] = {
    "eq": "equals",
    "lt": "less than",
    "le": "less than or equal",
    "gt": "greater than",
    "ge": "greater than or equal",
}

_DATE_FIELD_SPECS: List[tuple[str, str, str]] = [
    ("contentDateStart", "ContentDate/Start", "Acquisition start date"),
    ("contentDateEnd", "ContentDate/End", "Acquisition end date"),
    ("publicationDate", "PublicationDate", "Publication date"),
]

_OPERATOR_SUFFIXES: Dict[str, ODataComparisonOp] = {
    "Eq": "eq",
    "Lt": "lt",
    "Le": "le",
    "Gt": "gt",
    "Ge": "ge",
}

_DATE_FILTERS: Dict[str, DateFilterSpec] = {
    f"{base}{suffix}": DateFilterSpec(
        field,
        op,
        f"{desc} {_OPERATOR_LABELS[op]} ({field} {op})",
        interval_only=suffix == "",
    )
    for base, field, desc in _DATE_FIELD_SPECS
    for suffix, op in [("", "eq"), *_OPERATOR_SUFFIXES.items()]
}

_BUILTIN_PARAMS: Dict[str, Dict[str, str]] = {
    "name": {
        "title": "Filter by product name (substring match)",
        "example": "S2A_MSIL2A_20240110",
    },
    "geometry": {
        "title": "WKT geometry for spatial filtering",
        "example": "POLYGON((lon1 lat1, lon2 lat2, ...))",
    },
}

_INTERNAL_PARAMS = {"top", "skip"}

_TYPE_TO_ODATA_ATTR: Dict[str, ODataAttributeType] = {
    "String": "StringAttribute",
    "Integer": "IntegerAttribute",
    "Double": "DoubleAttribute",
    "DateTimeOffset": "DateTimeOffsetAttribute",
    "Boolean": "BooleanAttribute",
}

_DEPRECATED_PARAMS: Dict[str, str] = {
    "box": (
        "The 'box' parameter was only supported in the old OpenSearch API, "
        "use the 'geometry' parameter with a polygon in WKT format instead. "
        "Example: geometry='POLYGON((west south, west north, "
        "east north, east south, west south))'."
    ),
    "startDate": (
        "The 'startDate' parameter has been renamed. Use 'contentDateStartGt' instead."
    ),
    "completionDate": (
        "The 'completionDate' parameter has been renamed. "
        "Use 'contentDateEndLt' instead."
    ),
    "publishedAfter": (
        "The 'publishedAfter' parameter has been renamed. "
        "Use 'publicationDateGt' instead."
    ),
    "publishedBefore": (
        "The 'publishedBefore' parameter has been renamed. "
        "Use 'publicationDateLt' instead."
    ),
    "maxRecords": "The 'maxRecords' parameter has been renamed. Use 'top' instead.",
}


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


class FeatureQuery:  # pylint: disable=too-many-instance-attributes
    """
    An iterator over the features matching the search terms

    Queries the API in batches (default: MAX_BATCH_SIZE) features, and returns them
    one by one. Queries the next batch when the current batch is exhausted.
    """

    def __init__(
        self,
        collection: str,
        search_terms: Dict[str, SearchTermValue],
        proxies: Union[Dict[str, str], None] = None,
        options: Union[Dict[str, Any], None] = None,
    ) -> None:
        opts = options or {}
        self.total_results = -1
        self.features: List[Dict[str, Any]] = []
        self.proxies = proxies
        self._max_attempts = opts.get("max_attempts", 10)
        self.log = opts.get("logger") or NoopLogger()
        self.collection = collection
        self.search_terms = search_terms
        # Option to expand Attributes for product metadata (default: False)
        self.expand_attributes = opts.get("expand_attributes", False)
        self._initial_skip = _to_int(search_terms.get("skip", 0))
        self._top = _to_int(search_terms.get("top", MAX_BATCH_SIZE))
        if self._top > MAX_BATCH_SIZE:
            self.log.warning(
                f"Maximum 'top' value is {MAX_BATCH_SIZE}, setting to {MAX_BATCH_SIZE}"
            )
            self._top = MAX_BATCH_SIZE
        self.next_url = self._build_query_url(include_count=True)

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

    def _build_query_url(self, include_count: bool = False) -> str:
        """Build query URL with current skip offset"""
        filter_expr = _build_odata_filter(self.collection, self.search_terms)
        params = [
            f"$filter={quote(filter_expr)}",
            f"$top={self._top}",
            # Ordering for consistent pagination
            "$orderby=ContentDate/Start%20asc",
        ]
        if self._initial_skip > 0:
            params.append(f"$skip={self._initial_skip}")
        if include_count:
            params.append("$count=true")
        # Optionally expand Attributes to get product metadata
        # (productType, cloudCover, etc.)
        if self.expand_attributes:
            params.append("$expand=Attributes")
        return (
            "https://catalogue.dataspace.copernicus.eu/odata/v1/Products?"
            f"{'&'.join(params)}"
        )

    def __fetch_features(self) -> None:
        if self.next_url is None:
            return
        session = Credentials.make_session(
            None, False, Credentials.RETRIES, self.proxies
        )
        attempts = 0
        while attempts < self._max_attempts:
            attempts += 1
            try:
                assert self.next_url is not None  # for type checker
                with session.get(self.next_url) as response:
                    if response.status_code != 200:
                        retrying = attempts < self._max_attempts
                        self.log.warning(
                            f"Status code {response.status_code}, "
                            f"{'retrying...' if retrying else 'aborting'}"
                        )
                        if retrying:
                            sleep(60 * (1 + (random() / 4)))
                        continue
                    odata_response = response.json()
                    products = odata_response.get("value", [])
                    # Add Collection attribute for download_feature()
                    for product in products:
                        product["Collection"] = self.collection
                    self.features.extend(products)
                    total_results = odata_response.get("@odata.count")
                    if total_results is not None:
                        self.total_results = total_results
                    elif self.total_results < 0:
                        self.log.error("Total result count not present in response.")
                    next_link = odata_response.get("@odata.nextLink")
                    self.next_url = (
                        _strip_odata_count(next_link)
                        if next_link and self._top > 0
                        else None
                    )
                    return
            except (ChunkedEncodingError, ConnectionResetError, ProtocolError) as e:
                self.log.warning(e)
                continue
        self.log.error("Failed to fetch features after %d attempts", attempts)
        self.next_url = None


def query_features(
    collection: str,
    search_terms: Dict[str, SearchTermValue],
    proxies: Union[Dict[str, str], None] = None,
    options: Union[Dict[str, Any], None] = None,
) -> FeatureQuery:
    """
    Returns an iterator over the features matching the search terms
    """
    return FeatureQuery(collection, search_terms, proxies, options)


def shape_to_wkt(shape: str) -> str:
    """
    Convert a shapefile to a WKT string
    """
    # pylint: disable=line-too-long
    coordinates = list(gpd.read_file(shape).geometry[0].exterior.coords)  # pyright:ignore[reportAttributeAccessIssue]
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


def describe_search_terms() -> Dict[str, Dict[str, str]]:
    """Get builtin search terms (date filters, geometry) that are always available.

    Returns only the builtin parameters. To get collection-specific attributes,
    use describe_collection() with a collection name.
    """
    terms: Dict[str, Dict[str, str]] = {
        key: {"title": spec.title, "example": "2024-01-01 or 2024-01-01T00:00:00Z"}
        for key, spec in _DATE_FILTERS.items()
        if not spec.interval_only
    }
    terms.update(_BUILTIN_PARAMS)
    return terms


def _fetch_collection_attributes(
    collection: str,
    proxies: Union[Dict[str, str], None] = None,
    options: Union[Dict[str, Any], None] = None,
) -> Optional[List[Dict[str, str]]]:
    """Fetch available attributes for a collection from the OData API."""
    url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Attributes({collection})"
    log = (options or {}).get("logger") or NoopLogger()
    session = Credentials.make_session(None, False, Credentials.RETRIES, proxies)
    response = session.get(url, timeout=30)
    if response.status_code == 404:
        log.error("Collection '%s' not found", collection)
    elif response.status_code != 200:
        log.error(
            "Failed to fetch attributes for '%s': HTTP status code %s",
            collection,
            response.status_code,
        )
    return response.json() if response.status_code == 200 else None


def describe_collection(
    collection: str,
    proxies: Union[Dict[str, str], None] = None,
    options: Union[Dict[str, Any], None] = None,
) -> Optional[Dict[str, Dict[str, str]]]:
    """
    Get available filter parameters for a given collection.

    Fetches available attributes from the OData API's Attributes endpoint.
    Not all server attributes might be available as search terms at this time.

    Args:
        collection: Collection name (e.g., "SENTINEL-2", "SENTINEL-1")
        proxies: Optional proxy configuration
        options: Optional options

    Returns:
        Dictionary of parameters that can be used in filters (builtin + server attrs)
        or None if description could not be fetched
    """
    # Start with built-in search terms (base date names only, no Lt/Le/Gt/Ge variants)
    search_terms: Dict[str, Dict[str, str]] = {
        base: {"title": desc, "example": "2024-01-01 or 2024-01-01T00:00:00Z"}
        for base, _, desc in _DATE_FIELD_SPECS
    }
    search_terms.update(_BUILTIN_PARAMS)

    # Fetch attributes for the collection from the server
    if server_attributes := _fetch_collection_attributes(collection, proxies, options):
        for attr in server_attributes:
            if not (name := attr.get("Name")):
                continue
            entry: Dict[str, str] = {"type": attr.get("ValueType", "String")}
            # Use title from ATTRIBUTES if available
            if name in ATTRIBUTES and (title := ATTRIBUTES[name].get("Title")):
                entry["title"] = title
            search_terms[name] = entry
        return dict(sorted(search_terms.items()))
    return None


def get_product_attribute(
    product: Dict[str, Any], name: str, default: Optional[T] = None
) -> Optional[T]:
    """
    Get an attribute value from a product's Attributes array.

    Args:
        product: Product dictionary
        name: Attribute name to retrieve (e.g., 'cloudCover', 'productType')
        default: Value to return if attribute is not found (default: None)

    Returns:
        The attribute value if found, default otherwise
    """
    for attr in product.get("Attributes", []):
        if attr.get("Name") == name:
            return attr.get("Value")
    return default


def _parse_interval(
    value: str,
) -> Optional[tuple[str, str, ODataComparisonOp, ODataComparisonOp]]:
    """Parse interval syntax like [a,b], (a,b), [a,b), (a,b].

    Returns:
        Tuple of (start_value, end_value, start_op, end_op) or None if not an interval.
        start_op is 'ge' for '[' or 'gt' for '('
        end_op is 'le' for ']' or 'lt' for ')'
    """
    value = value.strip()
    if len(value) < 3:
        return None
    start_char = value[0]
    end_char = value[-1]
    if start_char not in "[(" or end_char not in "])":
        return None

    inner = value[1:-1]
    parts = inner.split(",")
    if len(parts) != 2:
        return None

    start_value = parts[0].strip()
    end_value = parts[1].strip()
    if not start_value or not end_value:
        return None

    # Determine operators based on brackets
    start_op = "ge" if start_char == "[" else "gt"
    end_op = "le" if end_char == "]" else "lt"
    return start_value, end_value, start_op, end_op


def _parse_operator_suffix(key: str) -> tuple[str, ODataComparisonOp]:
    """Parse operator suffix from a key like 'cloudCoverLt'."""
    for suffix, operator in _OPERATOR_SUFFIXES.items():
        if key.endswith(suffix):
            base_name = key[: -len(suffix)]
            return base_name, operator
    return key, "eq"


def _build_generic_attribute_filters(key: str, str_value: str) -> List[str]:
    """Build OData filter expression(s) for a generic attribute parameter."""
    # Check if key has operator suffix (e.g., cloudCoverLt, orbitNumberGe)
    base_name, operator = _parse_operator_suffix(key)
    if not (attr_info := ATTRIBUTES.get(base_name)):
        raise ValueError(f"The '{key}' parameter is not supported.")
    attr_type = attr_info.get("Type", "String")
    if not (odata_attr_type := _TYPE_TO_ODATA_ATTR.get(attr_type)):
        raise ValueError(
            f"Unsupported attribute type '{attr_type}' for parameter '{key}'."
        )
    # String and Boolean attributes only support equality
    if attr_type in ("String", "Boolean") and operator != "eq":
        raise ValueError(
            f"Comparison operators are not supported on {attr_type.lower()} "
            f"attribute '{base_name}'."
        )
    # Check for interval syntax (only for numeric and date types)
    if attr_type in ("Integer", "Double", "DateTimeOffset"):
        has_suffix = key != base_name
        interval = _parse_interval(str_value)
        if not has_suffix:
            if not interval:
                raise ValueError(
                    f"'{key}' requires interval syntax, e.g. {key}=[a,b]. "
                    f"For an exact match, use '{key}Eq' instead."
                )
            start_str, end_str, start_op, end_op = interval
            return [
                _build_attribute_filter(
                    base_name, start_str, odata_attr_type, start_op
                ),
                _build_attribute_filter(base_name, end_str, odata_attr_type, end_op),
            ]
        if interval:
            raise ValueError(
                f"Interval syntax is not allowed on '{key}'. "
                f"Use '{base_name}' for intervals instead."
            )
    return [_build_attribute_filter(base_name, str_value, odata_attr_type, operator)]


def _build_odata_filter(
    collection: str, search_terms: Dict[str, SearchTermValue]
) -> str:
    """Build $filter expression from search terms."""
    filters = [f"Collection/Name eq '{collection}'"]
    for key, value in search_terms.items():
        if key in _INTERNAL_PARAMS:
            continue
        if deprecated_message := _DEPRECATED_PARAMS.get(key):
            raise ValueError(deprecated_message)
        str_value = (
            _format_odata_date(value)
            if isinstance(value, (datetime, date))
            else str(value)
        )
        if spec := _DATE_FILTERS.get(key):
            interval = _parse_interval(str_value)
            if spec.interval_only:
                if not interval:
                    raise ValueError(
                        f"'{key}' requires interval syntax, e.g. {key}=[a,b]. "
                        f"For an exact match, use '{key}Eq' instead."
                    )
                start_str, end_str, start_op, end_op = interval
                filters.append(f"{spec.odata_field} {start_op} {start_str}")
                filters.append(f"{spec.odata_field} {end_op} {end_str}")
            else:
                if interval:
                    raise ValueError(
                        f"Interval syntax is not allowed on '{key}'. "
                        f"Use the base name for intervals instead."
                    )
                filters.append(f"{spec.odata_field} {spec.operator} {str_value}")
        elif key in ("name", "nameEq"):
            filters.append(f"contains(Name,'{str_value}')")
        elif key in ("geometry", "geometryEq"):
            filters.append(
                f"OData.CSC.Intersects(area=geography'SRID=4326;{str_value}')"
            )
        else:
            filters.extend(_build_generic_attribute_filters(key, str_value))
    return " and ".join(filters)


def _format_odata_date(date_value: Union[date, datetime]) -> str:
    """Format date value for OData filter expressions"""
    if isinstance(date_value, datetime):
        return date_value.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return f"{date_value.strftime('%Y-%m-%d')}T00:00:00.000Z"


def _to_odata_value_str(
    value: str, attr_type: ODataAttributeType, attr_name: str
) -> str:
    """Convert a string value to its OData string representation."""
    if attr_type == "StringAttribute":
        return f"'{value}'"
    if attr_type == "DoubleAttribute":
        return str(float(value))
    if attr_type == "IntegerAttribute":
        return str(int(value))
    if attr_type == "DateTimeOffsetAttribute":
        return value
    if attr_type == "BooleanAttribute":
        lower = value.lower()
        if lower not in ("true", "false"):
            raise ValueError(
                f"Invalid boolean value '{value}' for attribute '{attr_name}'. "
                "Use 'true' or 'false'."
            )
        return lower
    return value


def _build_attribute_filter(
    attr_name: str,
    attr_value: str,
    attr_type: ODataAttributeType,
    operator: ODataComparisonOp,
) -> str:
    value_str = _to_odata_value_str(attr_value, attr_type, attr_name)
    return (
        f"Attributes/OData.CSC.{attr_type}/any(att:att/Name eq '{attr_name}' and "
        f"att/OData.CSC.{attr_type}/Value {operator} {value_str})"
    )


def _to_int(value: SearchTermValue) -> int:
    """Convert a search term value to int, accepting only int or str."""
    if isinstance(value, (int, str)) and not isinstance(value, bool):
        return int(value)
    raise ValueError(f"Expected int or str, got {type(value).__name__}: {value!r}")


def _strip_odata_count(url: str) -> str:
    """Remove $count=true from a URL to avoid requesting count on every page."""
    url = re.sub(r"[&?](\$|%24)count=true", "", url, count=1)
    # If the first param was removed, the next '&' must become '?'
    if "?" not in url and "&" in url:
        url = url.replace("&", "?", 1)
    return url
