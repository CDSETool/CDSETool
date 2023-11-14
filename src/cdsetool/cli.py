"""
Command line interface
"""
import json as JSON
from typing import List, Optional
from typing_extensions import Annotated
import typer
from cdsetool.query import describe_collection, query_features
from cdsetool.monitor import StatusMonitor
from cdsetool.download import download_features

app = typer.Typer()

query_app = typer.Typer()
app.add_typer(query_app, name="query")

query_app = typer.Typer()
app.add_typer(query_app, name="query")


@query_app.command("search-terms")
def query_search_terms(collection: str):
    """
    List the available search terms for a collection
    """
    print(f"Available search terms for collection {collection}:")
    # TODO: print validators
    for key, _ in describe_collection(collection).items():
        print(f"\t- {key}")


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
):
    """
    Search for features matching the search terms
    """
    search_term = search_term or []
    search_term = _to_dict(search_term)
    features = query_features(collection, {**search_term})

    for feature in features:
        if json:
            print(JSON.dumps(feature))
        else:
            print(feature.get("properties").get("title"))


# TODO: implement limit
@app.command("download")
def download(
    collection: str,
    path: str,
    concurrency: Annotated[
        int, typer.Option(help="Number of concurrent connections")
    ] = 1,
    search_term: Annotated[
        Optional[List[str]],
        typer.Option(
            help="Search by term=value pairs. "
            + "Pass multiple times for multiple search terms"
        ),
    ] = None,
):
    """
    Download all features matching the search terms
    """
    search_term = search_term or []
    search_term = _to_dict(search_term)
    features = query_features(collection, {**search_term})

    list(
        download_features(
            features, path, {"monitor": StatusMonitor, "concurrency": concurrency}
        )
    )


def main():
    """
    Main entry point
    """
    app()


def _to_dict(term_list: List[str]):
    search_terms = {}
    for item in term_list:
        key, value = item.split("=")
        search_terms[key] = value
    return search_terms
