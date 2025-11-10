"""
Command line interface
"""

import json as JSON
import os
import sys
from typing import Dict, List, Optional

import typer
from typing_extensions import Annotated

from cdsetool.download import download_features
from cdsetool.monitor import StatusMonitor
from cdsetool.query import describe_collection, query_features

app = typer.Typer(no_args_is_help=True)

query_app = typer.Typer(no_args_is_help=True)
app.add_typer(query_app, name="query")

def subset_last_baseline(features):
    startDate = []
    processingBaseline = []
    for i in range(len(features)):
        startDate.append(features[i].get("properties").get("startDate").split(".")[0])
        processingBaseline.append(features[i].get("properties").get("processingBaseline"))
    fi = list(map(lambda x: startDate.index(x), startDate))
    fl = []
    for d in set(fi):
        ff = [i for i,x in enumerate(fi) if x==d]
        if len(ff) == 1:
            fl.append(int("".join(map(str, ff))))
        else:
            pBl = [processingBaseline[i] for i in ff]
            for b in range(0, len(pBl)):
                if pBl[b] < 9900 and pBl[b] == max(pBl):
                    fl.append(int("".join(str(ff[b]))))
    sub_features = [features[i] for i in fl]
    return(sub_features)

@query_app.command("search-terms")
def query_search_terms(collection: str) -> None:
    """
    List the available search terms for a collection
    """
    print(f"Available search terms for collection {collection}:")
    # TODO: print validators
    for key, attributes in describe_collection(collection).items():
        print(f"  - {key}")
        if attributes.get("title"):
            print(f"    - Description: {attributes.get('title')}")
        if attributes.get("pattern"):
            print(f"    - Pattern: {attributes.get('pattern')}")
        if attributes.get("minInclusive"):
            print(f"    - Min: {attributes.get('minInclusive')}")
        if attributes.get("maxInclusive"):
            print(f"    - Max: {attributes.get('maxInclusive')}")

        print()


# TODO: implement limit
@query_app.command("search")
def query_search(
    collection: str,
    search_term: Annotated[
        Optional[List[str]],
        typer.Option(
            help="Search by term=value pairs. "
            + "Pass multiple times for multiple search terms"
        ),
    ] = None,
    json: Annotated[bool, typer.Option(help="Output JSON")] = False,
) -> None:
    """
    Search for features matching the search terms
    """
    search_term = search_term or []
    features = query_features(collection, _to_dict(search_term))

    for feature in features:
        if json:
            print(JSON.dumps(feature))
        else:
            print(feature.get("properties").get("title"))


# TODO: implement limit
@app.command("download")
def download(  # pylint: disable=[too-many-arguments, too-many-positional-arguments]
    collection: str,
    path: str,
    concurrency: Annotated[
        int, typer.Option(help="Number of concurrent connections")
    ] = 1,
    overwrite_existing: Annotated[
        bool, typer.Option(help="Overwrite already downloaded files")
    ] = False,
    last_baseline: Annotated[
        bool, typer.Option(help="Download only last available processing baseline (only applicable to 'Sentinel2' collection)")
    ] = False,
    search_term: Annotated[
        Optional[List[str]],
        typer.Option(
            help="Search by term=value pairs. "
            + "Pass multiple times for multiple search terms"
        ),
    ] = None,
    filter_pattern: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Download specific files within product bundles using OData API's node"
                " filtering functionality"
            )
        ),
    ] = None,
) -> None:
    """
    Download all features matching the search terms
    """
    if not os.path.exists(path):
        print(f"Path {path} does not exist")
        sys.exit(1)

    search_term = search_term or []
    features = query_features(collection, _to_dict(search_term))

    if collection == 'Sentinel2' and last_baseline == True:
        features = subset_last_baseline(features)

    list(
        download_features(
            features,
            path,
            {
                "monitor": StatusMonitor(),
                "concurrency": concurrency,
                "overwrite_existing": overwrite_existing,
                "filter_pattern": filter_pattern,
            },
        )
    )


def main():
    """
    Main entry point
    """
    app()


def _to_dict(term_list: List[str]) -> Dict[str, str]:
    search_terms = {}
    for item in term_list:
        key, value = item.split("=")
        search_terms[key] = value
    return search_terms


if __name__ == "__main__":
    main()
