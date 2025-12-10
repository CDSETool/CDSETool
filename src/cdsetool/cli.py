"""
Command line interface
"""

import json as JSON
import os
import sys
from typing import Annotated, Dict, List, Optional

import typer

from cdsetool.download import download_features
from cdsetool.monitor import StatusMonitor
from cdsetool.query import (
    SearchTermValue,
    describe_collection,
    describe_search_terms,
    query_features,
)

app = typer.Typer(no_args_is_help=True)

query_app = typer.Typer(no_args_is_help=True)
app.add_typer(query_app, name="query")


def _format_attributes(attributes: Dict[str, Dict[str, str]]) -> str:
    """Format attribute details into a readable string."""
    lines = []
    for key, attr in attributes.items():
        lines.append(f"  - {key}")
        if title := attr.get("title"):
            lines.append(f"      Description: {title}")
        if attribute_type := attr.get("type"):
            lines.append(f"      Type: {attribute_type}")
        if example := attr.get("example"):
            lines.append(f"      Example: {example}")
    return "\n".join(lines)


@query_app.command("search-terms")
def query_search_terms(
    collection: Annotated[
        Optional[str],
        typer.Argument(
            help="Collection name (e.g., SENTINEL-1, SENTINEL-2). "
            "If omitted, shows only builtin parameters without querying the server."
        ),
    ] = None,
) -> None:
    """
    List the available search terms for a collection
    """
    if collection is None:
        # No collection specified - show only builtin params (no API call)
        print("Builtin search terms (use with --search-term):")
        print()
        print(_format_attributes(describe_search_terms()))
        print()
        print("Specify a collection name to see collection-specific attributes.")
    else:
        # Collection specified - fetch from API and show all supported params
        print(f"Search terms for collection {collection}:")
        print()
        if search_terms := describe_collection(collection):
            print(_format_attributes(search_terms))
        else:
            print("  (none)")


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
            print(feature.get("Name"))


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


def _to_dict(term_list: List[str]) -> Dict[str, SearchTermValue]:
    search_terms = {}
    for item in term_list:
        key, value = item.split("=", 1)  # Split on first = only
        search_terms[key] = value
    return search_terms


if __name__ == "__main__":
    main()
