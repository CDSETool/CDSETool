The odata test data has been fetched from Copernicus using the following query
to limit the match result:

query = query_features("SENTINEL-1", {"top": 10, "contentDateStartGt": "2014-04-05T00:00:00.000Z", "contentDateStartLt": "2014-04-14T00:00:00.000Z"}, options={"expand_attributes": True})

The original urls for first page (with request to get total count):
https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name%20eq%20%27SENTINEL-1%27%20and%20ContentDate/Start%20gt%202014-04-05T00%3A00%3A00.000Z%20and%20ContentDate/Start%20lt%202014-04-14T00%3A00%3A00.000Z&$top=10&$count=true&$expand=Attributes&$orderby=ContentDate/Start%20asc

The original URL for the second page (without request to get total count):
https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name%20eq%20%27SENTINEL-1%27%20and%20ContentDate/Start%20gt%202014-04-05T00:00:00.000Z%20and%20ContentDate/Start%20lt%202014-04-14T00:00:00.000Z&$top=10&$skip=10&$expand=Attributes&$orderby=ContentDate/Start%20asc

The original URL for the attributes:
https://catalogue.dataspace.copernicus.eu/odata/v1/Attributes(SENTINEL-1)
