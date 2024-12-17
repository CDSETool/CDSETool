"""
Download specific files within a product bundle.

Provides a function to search through a given pattern in a product node tree,
and a function to download specific nodes within a product.
"""

import fnmatch
import json
import os
from typing import Any, Dict, List, Union

import requests

from cdsetool.credentials import Credentials


def search_nodes(
    node_url: str, pattern: str, exclude: bool = False
) -> List[Dict[str, Any]]:
    """
    Search for a given pattern in product node tree obtained from a CDSE OData API url.

    If "exclude" is set to False, only nodes that match pattern are returned. If it's
    set to True, only nodes that do not match pattern are returned.
    """
    input_nodes = json.loads(requests.get(node_url, timeout=20).text)["result"]
    output_nodes = []
    for node in input_nodes:
        # If this node is a dir, call 'search_nodes' recursively
        if node["ContentLength"] == 0:
            output_nodes.extend(search_nodes(node["Nodes"]["uri"], pattern))

        # If it's a file, check for pattern match and optionally append to results
        elif node["ContentLength"] >= 0:
            match = fnmatch.fnmatch(node["Name"], pattern)
            if match and not exclude or exclude and not match:
                output_nodes.append(node)

    return output_nodes

def download_node(
    feature_id,
    path: str,
    username: str,
    password: str,
    nodefilter_pattern: str,
):
    """
    Download specific files within a feature using node filtering.
    """
    url = (
        "https://download.dataspace.copernicus.eu/"
        f"odata/v1/Products({feature_id})/Nodes"
    )
    nodes = search_nodes(url, nodefilter_pattern)

    for node in nodes:
        # Authenticate to CDSE
        session = Credentials(username, password).get_session()

        url = f"{node['Nodes']['uri'][:-5]}$value"
        response = session.get(url, stream=True)

        # Download file if request was successful
        if response.status_code == 200:
            with open(
                os.path.join(path, node["Name"]),
                "wb",  # TODO: Reproduce directories tree structure
            ) as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)
        else:
            print(f"Failed to download file. Status code: {response.status_code}")
            print(response.text)
