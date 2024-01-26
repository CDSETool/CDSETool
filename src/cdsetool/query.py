"""
Query the Copernicus Data Space Ecosystem OpenSearch API

https://documentation.dataspace.copernicus.eu/APIs/OpenSearch.html
"""
from xml.etree import ElementTree
from datetime import datetime, date
import re
import json
import requests
from collections import OrderedDict
from src.cdsetool.exceptions import (
    InvalidAoi,
    InvalidDataCollection,
    InvalidDirection,
    InvalidFormatDate,
    InvalidMode,
    InvalidOrbit,
    InvalidOrderBy,
    InvalidPLevel,
    InvalidProductType,
    InvalidTileId)
from src.cdsetool.helper import correct_data_collections, correct_format_date, valid_aoi, valid_direction, valid_mode, valid_orbit, valid_order_by, valid_plevel, valid_product_type, valid_tileId


class _FeatureIterator:
    def __init__(self, feature_query):
        self.index = 0
        self.feature_query = feature_query

    def __len__(self):
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

    total_results = None

    def __init__(self, collection, search_terms):
        self.features = []
        self.next_url = _query_url(collection, {**search_terms, "exactCount": "1"})

    def __iter__(self):
        return _FeatureIterator(self)

    def __len__(self):
        if self.total_results is None:
            self.__fetch_features()

        return self.total_results

    def __getitem__(self, index):
        while index >= len(self.features) and self.next_url is not None:
            self.__fetch_features()

        return self.features[index]

    def __fetch_features(self):
        if self.next_url is not None:
            res = requests.get(self.next_url, timeout=120).json()
            self.features += res.get("features") or []
            total_results = res.get("properties", {}).get("totalResults")
            if total_results is not None:
                self.total_results = total_results
            odata =self.__fetch_odata(res.get("properties", {}))
            if odata != {}:
                self.__add_odata_to_features(odata)
            self.__set_next_url(res)
    
    def __fetch_odata(self,properties):
        #TODO ADD all properties still not implemented, the ones descripted in describe_colleciton
        if properties != {}:
            if properties.get("query",{}) != {}:
                if properties.get("query").get("originalFilters",{})!= {}:
                    original_filters = properties.get("query").get("originalFilters")
                    odata_url = "https://catalogue.dataspace.copernicus.eu/odata/v1/"
                    start_date = original_filters.get("startDate")
                    end_date = original_filters.get("completionDate")
                    data_collection = original_filters.get("collection")
                    aoi = original_filters.get("geometry")
                    order_by = original_filters.get("orderBy", 'asc')
                    product_type = original_filters.get("productType",None)
                    mode = original_filters.get("sensorMode",None)
                    direction = original_filters.get("orbitDirection",None)
                    orbit = original_filters.get("orbitNumber",None)
                    tileid = original_filters.get("tileId", None)
                    plevel = original_filters.get("processingLevel",None)
                    odata_query = self.format_query(start_date= start_date,end_date= end_date,data_collection= data_collection,aoi= aoi,order_by= order_by,product_type=product_type,mode=mode,direction=direction,orbit=orbit,tileid=tileid,plevel=plevel)
                    if odata_query.strip() == "":
                        # An empty query should return the full set of products on the server, which is a bit unreasonable.
                        # The server actually raises an error instead and it's better to fail early in the client.
                        raise ValueError("Empty query.")
                    res = self.__query_call_ordered_dict(url=odata_url,query=odata_query)
                    return res
        #one of the filters is missing
        return {}
    
    def __query_call_ordered_dict(self,url=None,query=None)-> OrderedDict:
        total_url = url
        if query!= None:
            total_url += query
        json_ = requests.get(f"{url}{query}").json()
        if 'value' in json_:
            import json
            string_value = json.dumps(json_['value'])
            return json.JSONDecoder(object_pairs_hook=OrderedDict).decode(string_value)
        else:
            import json
            string_value = json.dumps(json_)
            return json.JSONDecoder(object_pairs_hook=OrderedDict).decode(string_value)
    
    def __add_odata_to_features(self,odata):
        """
        Given the odatadict, add the to correct (by matching the Ids) checksum the features
        """
        map_id_checksum = {}
        for ordered_dict in odata:
            map_id_checksum[ordered_dict["Id"]] = {"Checksum": ordered_dict["Checksum"], "ContentLength": ordered_dict["ContentLength"]}
        for feature in self.features:
            if feature.get("id") in map_id_checksum.keys():
                ordered_dict = map_id_checksum[feature.get("id")]
                feature['Checksum'] = ordered_dict["Checksum"]
                if feature.get('properties').get('services').get('download').get('size') == ordered_dict['ContentLength']:
                    feature['ContentLength'] = ordered_dict['ContentLength']
                else:
                    print(f"Warning: content length of {feature.get('id')} does not match the ContentLength in odata")
            else:
                print(f"Warning: {feature.get('id')} not in odata")

    def __set_next_url(self, res):
        links = res.get("properties", {}).get("links") or []
        self.next_url = next(
            (link for link in links if link.get("rel") == "next"), {}
        ).get("href")

        if self.next_url:
            self.next_url = self.next_url.replace("exactCount=1", "exactCount=0")
    
    @staticmethod
    def format_query(
        start_date= None,
        end_date= None,
        data_collection= None,
        aoi= None,
        product_type = None,
        mode = None,
        direction = None,
        orbit = None,
        plevel = None,
        tileid = None,
        order_by= None,
        ):
        """Create a OpenSearch API query string. ODATA"""
        pieces = []
        if data_collection != None:
            if correct_data_collections(data_collection):
                data_collection_query = f"$filter=Collection/Name eq '{data_collection}'"
                pieces.append(data_collection_query)
            else:
                error_msg = f"The data collection inderted is not one of the valid! {['Sentinel-1','Sentinel-2']}"
                raise(InvalidDataCollection(error_msg))
        if start_date != None:
            if correct_format_date(start_date):
                start_date_query = f"ContentDate/Start gt {start_date}"
                pieces.append(start_date_query)
            else:
                error_msg = f"The starting date inserted is not valid! {start_date} is not in the format required yyyy-mm-dd!"
                raise(InvalidFormatDate(error_msg))
        if end_date != None:
            if correct_format_date(end_date):
                end_date_query = f"ContentDate/Start lt {end_date}"
                pieces.append(end_date_query)
            else:
                error_msg = f"The ending date inserted is not valid! {end_date} is not in the format required yyyy-mm-dd!"
                raise(InvalidFormatDate(error_msg))
        if aoi!= None:
            if valid_aoi(aoi):
                aoi_query = f"OData.CSC.Intersects(area=geography'SRID=4326;{aoi}')"
                pieces.append(aoi_query)
            else:
                error_msg = "The aoi geometry inserted is not valid!"
                raise(InvalidAoi(error_msg))
        if product_type != None:
            if valid_product_type(product_type):
                product_type_query = f"Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' and att/OData.CSC.StringAttribute/Value eq '{product_type}')"
                pieces.append(product_type_query)
            else:
                error_msg = "The product type inserted is not valid!"
                raise(InvalidProductType(error_msg))
        if mode != None:
            if valid_mode(mode):
                mode_query = f"Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'operationalMode' and att/OData.CSC.StringAttribute/Value eq '{mode}')"
                pieces.append(mode_query)
            else:
                error_msg = "The mode inserted is not valid!"
                raise(InvalidMode(error_msg))
        if direction != None:
            if valid_direction(direction):
                direction_query = f"Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'orbitDirection' and att/OData.CSC.StringAttribute/Value eq '{direction}')"
                pieces.append(direction_query)
            else:
                error_msg = "The direction inserted is not valid!"
                raise(InvalidDirection(error_msg))
        if orbit != None:
            if valid_orbit(orbit):
                orbit_query = f"Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'relativeOrbitNumber' and att/OData.CSC.IntegerAttribute/Value eq {orbit})"
                pieces.append(orbit_query)
            else:
                error_msg = "The orbit inserted is not valid!"
                raise(InvalidOrbit(error_msg))
        if plevel != None:
            if valid_plevel(plevel):
                plevel_query = f"Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'processingLevel' and att/OData.CSC.StringAttribute/Value eq '{plevel}')"
                pieces.append(plevel_query)
            else:
                error_msg = "The plevel inserted is not valid!"
                raise(InvalidPLevel(error_msg))
        if tileid != None:
            if valid_tileId(tileid):
                tileId_query = f"Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'tileId' and att/OData.CSC.StringAttribute/Value eq '{tileid}'"
                pieces.append(tileId_query)
            else:
                error_msg = "The tileId by inserted is not valid!"
                raise(InvalidTileId(error_msg))    
        if order_by != None:
            if valid_order_by(order_by):
                order_by_query = f"&$orderby=ContentDate/Start {order_by}"
                pieces.append(order_by_query)
            else:
                error_msg = "The order_by by inserted is not valid!"
                raise(InvalidOrderBy(error_msg))
        else:
            # always order by ascending order if nothing provided
            order_by_query = f"&$orderby=ContentDate/Start asc"
            pieces.append(order_by_query)
        full_query = "Products?"
        for i in range(len(pieces)):
            full_query+= pieces[i]
            if i == len(pieces)-2 and valid_order_by(order_by):
                #order by query doen't need the "and"  
                pass
            elif i!= len(pieces)-1:
                full_query+= " and "
        return full_query


def query_features(collection, search_terms):
    """
    Returns an iterator over the features matching the search terms
    """
    return FeatureQuery(collection, {"maxRecords": 2000, **search_terms})


def shape_to_wkt(shape):
    """
    Convert a shapefile to a WKT string
    """
    try:
        import geopandas as gpd  # pylint: disable=import-outside-toplevel
    except ImportError:
        print(
            "geopandas is not installed. Please install it with `pip install geopandas`"
        )
    coordinates = list(gpd.read_file(shape).geometry[0].exterior.coords)
    return (
        "POLYGON(("
        + ", ".join(" ".join(map(str, coord)) for coord in coordinates)
        + "))"
    )


def geojson_to_wkt(geojson):
    """
    Convert a geojson geometry to a WKT string
    """
    if isinstance(geojson, str):
        geojson = json.loads(geojson)

    if geojson.get("type") == "Feature":
        geojson = geojson["geometry"]

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


def describe_collection(collection):
    """
    Get a list of valid options for a given collection in key value pairs
    """
    content = _get_describe_doc(collection)
    tree = ElementTree.fromstring(content)
    parameter_node_parent = tree.find(
        "{http://a9.com/-/spec/opensearch/1.1/}Url[@type='application/json']"
    )

    parameters = {}
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


def _query_url(collection, search_terms):
    description = describe_collection(collection)

    query_list = []
    for key, value in search_terms.items():
        val = _serialize_search_term(value)
        _validate_search_term(key, val, description)
        query_list.append(f"{key}={val}")

    return (
        "https://catalogue.dataspace.copernicus.eu"
        + f"/resto/api/collections/{collection}/search.json?{'&'.join(query_list)}"
    )


def _serialize_search_term(search_term):
    if isinstance(search_term, list):
        return ",".join(search_term)

    if isinstance(search_term, datetime):
        return search_term.strftime("%Y-%m-%dT%H:%M:%SZ")

    if isinstance(search_term, date):
        return search_term.strftime("%Y-%m-%d")

    return str(search_term)


def _validate_search_term(key, search_term, description):
    _assert_valid_key(key, description)
    _assert_match_pattern(search_term, description.get(key).get("pattern"))
    _assert_min_inclusive(search_term, description.get(key).get("minInclusive"))
    _assert_max_inclusive(search_term, description.get(key).get("maxInclusive"))


def _assert_valid_key(key, description):
    assert key in description.keys(), (
        f'search_term with name "{key}" '
        + "was not found for collection."
        + f" Available terms are: {', '.join(description.keys())}"
    )


def _assert_match_pattern(search_term, pattern):
    if not pattern:
        return

    assert re.match(
        pattern, search_term
    ), f"search_term {search_term} does not match pattern {pattern}"


def _assert_min_inclusive(search_term, min_inclusive):
    if not min_inclusive:
        return

    assert int(search_term) >= int(
        min_inclusive
    ), f"search_term {search_term} is less than min_inclusive {min_inclusive}"


def _assert_max_inclusive(search_term, max_inclusive):
    if not max_inclusive:
        return

    assert int(search_term) <= int(
        max_inclusive
    ), f"search_term {search_term} is greater than max_inclusive {max_inclusive}"


_describe_docs = {}


def _get_describe_doc(collection):
    if _describe_docs.get(collection):
        return _describe_docs.get(collection)

    res = requests.get(
        "https://catalogue.dataspace.copernicus.eu"
        + f"/resto/api/collections/{collection}/describe.xml",
        timeout=120,
    )
    assert res.status_code == 200, (
        f"Unable to find collection with name {collection}. Please see "
        + "https://documentation.dataspace.copernicus.eu"
        + "/APIs/OpenSearch.html#collections "
        + "for a list of available collections"
    )

    _describe_docs[collection] = res.content
    return _describe_docs.get(collection)
