# CDSETool

## About CDSETool
This script downloads copernicus data from the Copernicus Data Space Ecosystem

## Usage

### Querying features

Querying is always done in batches, returning `len(results) <= maxRecords` records each time.
a local buffer is filled and gradually emptied as results are yielded. When the buffer is empty,
more results will be requested and the process repeated until no more results are available, or
the iterator is discarded.

```python
from cdse.query import query_features

collection = "Sentinel2"
search_terms = {
    "maxRecords": "100", # batch size, between 1 and 2000 (default 50).
    "startDate": "1999-01-01",
    "processingLevel": "S2MSI1C"
}

# wait for a single batch to finish, yield results immediately
for feature in query_features(collection, search_terms):
    # do something with feature

# wait for all batch requests to complete, returning list
features = list(query_features(collection, search_terms))

# manually iterate
iter = query_features(collection, search_terms)

featureA = next(iter)
featureB = next(iter)
# ...
```

#### Querying by shapes

To query by shapes, you must first convert your shape to Well Known Text (WKT). The included
`shape_to_wkt` can solve this.

```python
from cdse.query import query_features, shape_to_wkt

geometry = shape_to_wkt("path/to/shape.shp")

features = query_features("Sentinel3", {"geometry": geometry})
```

#### Querying by lists of parameters

Most search terms only accept a single argument. To query by a list of arguments, loop the arguments
and pass them one by one to the query function.

```python
from cdse.query import query_features

tile_ids = ["32TPT", "32UPU", "32UPU", "31RFL", "37XDA"]

for tile_id in tile_ids:
    features = query_features("Sentinel2", {"tileId": tile_id})
    for feature in features:
        # do things with feature
```

#### Listing search terms

To get a list of all search terms for a given collection, you may either use the `describe_collection` function or
use the CLI:

```python
from cdsetool.query import describe_collection

search_terms = describe_collection("Sentinel2").keys()
print(search_terms)
```

And the CLI:
```bash
$ cdsetool query search-terms Sentinel2
```

### Downloading features

#### Authenticating

An account is required to download features from the Copernicus distribution service.

To authenticate using an account, instantiate `Credentials` and pass your username and password

```python
from cdsetool.credentials import Credentials

username = "konata@izumi.com"
password = "password123"
credentials = Credentials(username, password)
```

Alternatively, `Credentials` can pull from `~/.netrc` when username and password are left blank.

```python
# ~/.netrc
machine https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token
login konata@izumi.com
password password123

# main.py
from cdsetool.credentials import Credentials

credentials = Credentials()
```

The credentials object may then be passed to a download function. If left out, the download
functions will default to using `.netrc`.

```python
credentials = Credentials()

download_features(features, "/some/download/path", {"credentials": credentials})
```

#### Concurrently downloading features

CDSETool provides a method for concurrently downloading features. The concurrency level
should match your accounts privileges

The downloaded feature ids are yielded, so its required to await the results.

```python
from cdsetool.query import query_features
from cdsetool.download import download_features

features = query_features("Sentinel2")

download_path = "/path/to/download/folder"
downloads = download_features(features, download_path, {"concurrency": 4})

for id in downloads:
    print(f"feature {id} downloaded")

# or

list(downloads)
```

#### Sequentially downloading features

Its possible to download features sequentially in a single thread if desired.

```python
from cdsetool.query import query_features
from cdsetool.download import download_feature

features = query_features("Sentinel2")

download_path = "/path/to/download/folder"
for feature in features:
    download_feature(feature, download_path)
```




