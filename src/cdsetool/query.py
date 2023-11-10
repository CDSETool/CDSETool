import requests
from xml.etree import ElementTree

class FeatureQuery:
    def __init__(self, collection, search_terms):
        self.features = []
        self.next_url = _query_url(collection, search_terms)

    def __iter__(self):
        return self
    
    def __next__(self):
        if len(self.features) == 0:
            if self.next_url == None:
                raise StopIteration
            res = requests.get(self.next_url).json()
            self.features += res["features"]
            self.next_url = next((link for link in res["properties"]["links"] if link.get("rel") == "next"), {}).get("href")
        return self.features.pop(0)


def query_features(collection, search_terms):
    return FeatureQuery(collection, search_terms)

def shape_to_wkt(shape):
    try:
        import geopandas as gpd
    except ImportError:
        print("geopandas is not installed. Please install it with `pip install geopandas`")
    exterior = gpd.read_file(shape).geometry[0].exterior
    coordinates = list(exterior.coords)
    odata_str = "POLYGON((" + ", ".join(" ".join(map(str, coord)) for coord in coordinates) + "))"

    return(odata_str)

def _query_url(collection, search_terms):
    _validate_search_terms(collection, search_terms)

    query_list = []
    for key, value in search_terms.items():
        query_list.append(f"{key}={value}")

    return f"https://catalogue.dataspace.copernicus.eu/resto/api/collections/{collection}/search.json?{'&'.join(query_list)}"

def _validate_search_terms(collection, search_terms):
    description = describe_collection(collection)
    keys = description.keys()
    for key in search_terms.keys():
        assert key in keys, f"search_term with name \"{key}\" was not found for collection \"{collection}\". Available terms are: {', '.join(keys)}"
        # TODO: validate patterns, minInclude, maxInclusive

def describe_collection(collection):
    content = _get_describe_doc(collection)
    tree = ElementTree.fromstring(content)
    parameter_node_parent = tree.find("{http://a9.com/-/spec/opensearch/1.1/}Url[@type='application/json']")

    parameters = {}
    for parameter_node in parameter_node_parent:
        name = parameter_node.attrib.get("name")
        if name:
            parameters[name] = True # TODO: replace with available options

    return parameters

_describe_docs = {}
def _get_describe_doc(collection):
    if _describe_docs.get(collection):
        return _describe_docs.get(collection)

    res = requests.get(f"https://catalogue.dataspace.copernicus.eu/resto/api/collections/{collection}/describe.xml")
    assert res.status_code == 200, f"Unable to find collection with name {collection}. Please see https://documentation.dataspace.copernicus.eu/APIs/OpenSearch.html#collections for a list of available collections"
    
    _describe_docs[collection] = res.content
    return _describe_docs.get(collection)
