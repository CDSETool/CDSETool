from cdsetool.query import describe_collection, query_features
from cdsetool.monitor import StatusMonitor
from cdsetool.download import download_features
import typer
import signal
import sys
from typing import List, Optional
from typing_extensions import Annotated
import json as JSON

app = typer.Typer()

query_app = typer.Typer()
app.add_typer(query_app, name="query")

query_app = typer.Typer()
app.add_typer(query_app, name="query")


@query_app.command("search-terms")
def query_search_terms(collection: str):
    print(f"Available search terms for collection {collection}:")
    # TODO: print validators
    for key, value in describe_collection(collection).items():
        print(f"\t- {key}")


# TODO: implement limit
@query_app.command("search")
def query_search(
    collection: str,
    limit: Annotated[int, typer.Option(help="Limit the number of results")] = 10,
    search_term: Annotated[
        Optional[List[str]],
        typer.Option(
            help="Search by term=value pairs. Pass multiple times for multiple search terms"
        ),
    ] = [],
    json: Annotated[bool, typer.Option(help="Output JSON")] = False,
):
    search_term = to_dict(search_term)
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
            help="Search by term=value pairs. Pass multiple times for multiple search terms"
        ),
    ] = [],
    json: Annotated[bool, typer.Option(help="Output JSON")] = False,
):
    search_term = to_dict(search_term)
    features = query_features(collection, {**search_term})

    list(
        download_features(
            features, path, {"monitor": StatusMonitor, "concurrency": concurrency}
        )
    )


def main():
    app()


def to_dict(l):
    d = {}
    for item in l:
        key, value = item.split("=")
        d[key] = value
    return d
