# CDSETool

## About CDSETool
This script downloads copernicus data from the Copernicus Data Space Ecosystem

## Quick start

```python
from cdsetool.query import query_features, shape_to_wkt
from cdsetool.credentials import Credentials
from cdsetool.download import download_features
from cdsetool.monitor import StatusMonitor
from datetime import date

features = query_features(
    "Sentinel1",
    {
        "startDate": "2020-12-20",
        "completionDate": date(2020, 12, 25),
        "processingLevel": "LEVEL1",
        "sensorMode": "IW",
        "productType": "IW_GRDH_1S",
        "geometry": shape_to_wkt("path/to/shapefile.shp"),
    },
)

list(
    download_features(
        features,
        "path/to/output/folder/",
        {
            "concurrency": 4,
            "monitor": StatusMonitor(),
            "credentials": Credentials("username", "password"),
        },
    )
)
```

Or use the CLI:

```bash
cdsetool query search Sentinel2 --search-term startDate=2020-01-01 --search-term completionDate=2020-01-10 --search-term processingLevel=S2MSI2A --search-term box="4","51","4.5","52"

cdsetool download Sentinel2 PATH/TO/DIR --concurrency 4 --search-term startDate=2020-01-01 --search-term completionDate=2020-01-10 --search-term processingLevel=S2MSI2A --search-term box="4","51","4.5","52"
```

## Table of Contents

- [CDSETool](#cdsetool)
  * [About CDSETool](#about-cdsetool)
  * [Quick Start](#quick-start)
  * [Table of Contents](#table-of-contents)
  * [Installatson](#installation)
  * [Usage](#usage)
    + [Querying features](#querying-features)
      - [Querying by shapes](#querying-by-shapes)
      - [Querying by lists of parameters](#querying-by-lists-of-parameters)
      - [Querying by dates](#querying-by-dates)
      - [Listing search terms](#listing-search-terms)
    + [Downloading features](#downloading-features)
      - [Authenticating](#authenticating)
      - [Concurrently downloading features](#concurrently-downloading-features)
      - [Sequentially downloading features](#sequentially-downloading-features)
  * [Roadmap](#roadmap)
  * [Contributing](#contributing)
  * [LICENSE](#license)

## Installation

Install `cdsetool` using pip:

```bash
pip install cdsetool==0.2.6
```

## Usage

### Querying features

Querying is always done in batches, returning `len(results) <= maxRecords` records each time.
A local buffer is filled and gradually emptied as results are yielded. When the buffer is empty,
more results will be requested and the process repeated until no more results are available, or
the iterator is discarded.

Since downloading features is the most common use-case, `query_features` assumes that the query will run till the end.
Because of this, the batch size is set to `2000`, which is the size limit set by CDSE.

```python
from cdsetool.query import query_features

collection = "Sentinel2"
search_terms = {
    "maxRecords": "100", # batch size, between 1 and 2000 (default: 2000).
    "startDate": "1999-01-01",
    "processingLevel": "S2MSI1C"
}

# wait for a single batch to finish, yield results immediately
for feature in query_features(collection, search_terms):
    # do something with feature

# wait for all batch requests to complete, returning list
features = list(query_features(collection, search_terms))

# manually iterate
iterator = query_features(collection, search_terms)

featureA = next(iterator)
featureB = next(iterator)
# ...
```

#### Querying by shapes

To query by shapes, you must first convert your shape to Well Known Text (WKT). The included
`shape_to_wkt` can solve this.

```python
from cdsetool.query import query_features, shape_to_wkt

geometry = shape_to_wkt("path/to/shape.shp")

features = query_features("Sentinel3", {"geometry": geometry})
```

#### Querying by lists of parameters

Most search terms only accept a single argument. To query by a list of arguments, loop the arguments
and pass them one by one to the query function.

```python
from cdsetool.query import query_features

tile_ids = ["32TPT", "32UPU", "32UPU", "31RFL", "37XDA"]

for tile_id in tile_ids:
    features = query_features("Sentinel2", {"tileId": tile_id})
    for feature in features:
        # do things with feature
```

#### Querying by dates

Its quite common to query for features created before, after or between dates.

```python
from cdsetool.query import query_features
from datetime import date, datetime

date_from = date(2020, 1, 1) # or datetime(2020, 1, 1, 23, 59, 59, 123456) or "2020-01-01" or "2020-01-01T23:59:59.123456Z"
date_to = date(2020, 12, 31)

features = query_features("Sentinel2", {"startDate": date_from, "completionDate": date_to})
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

Credentials can be validated using the `validate_credentials` function which will return a boolean.

```python
from cdsetool.credentials import validate_credentials

validate_credentials(username='user', password='password')
```

If None are passed to username and password, `validate_credentials` will validate `.netrc`

#### Concurrently downloading features

CDSETool provides a method for concurrently downloading features. The concurrency level
should match your accounts privileges. 
See [CDSE quotas](https://documentation.dataspace.copernicus.eu/Quotas.html)

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

## Roadmap

- [X] Query schema validation
- [ ] High-level API
    - [ ] Query features
    - [ ] Download features
        - [ ] Download single feature
        - [ ] Download list of features
        - [ ] Download by ID
        - [ ] Download by URL
- [ ] Command-Line Interface
    - [ ] Update to match the high-level API
    - [ ] Better `--help` messages
    - [ ] Quickstart guide in README.md
- [ ] Test suite
    - [ ] Query
    - [ ] Credentials
    - [ ] Download
    - [ ] Monitor
- [ ] Strategy for handling HTTP and connection errors

## Contributing

Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request.
You can also simply open an issue with the tag "enhancement".

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/cool-new-feature`)
3. Commit your Changes (`git commit -m 'Add some feature'`)
4. Push to the Branch (`git push origin feature/cool-new-feature`)
5. Open a Pull Request

## LICENSE

Distributed under the MIT License. See [LICENSE](LICENSE) for more information.
