"""Attribute definitions for CDSE OData product queries."""

from typing import Dict, List, TypedDict


class AttributeInfo(TypedDict, total=False):
    """Lookup entry for a product attribute's type and supported collections."""

    Type: str
    Title: str
    Collections: List[str]


ATTRIBUTES: Dict[str, AttributeInfo] = {
    "USGScollection": {
        "Type": "String",
        "Collections": [
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "acquisitionType": {
        "Type": "String",
        "Collections": [
            "SENTINEL-5P",
        ],
    },
    "authority": {
        "Type": "String",
        "Collections": [
            "SENTINEL-6",
            "SENTINEL-1-RTC",
            "ENVISAT",
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "baselineCollection": {
        "Type": "String",
        "Collections": [
            "SENTINEL-3",
            "SENTINEL-5P",
        ],
    },
    "brightCover": {
        "Type": "Double",
        "Collections": [
            "SENTINEL-3",
        ],
    },
    "card4lSpecification": {
        "Type": "String",
        "Collections": [
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "card4lSpecificationVersion": {
        "Type": "String",
        "Collections": [
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "closedSeaCover": {
        "Type": "Integer",
        "Collections": [
            "SENTINEL-3",
        ],
    },
    "cloudCover": {
        "Type": "Double",
        "Title": "Cloud cover percentage (0-100)",
        "Collections": [
            "SENTINEL-2",
            "SENTINEL-3",
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "cloudCoverLand": {
        "Type": "Double",
        "Collections": [
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "coastalCover": {
        "Type": "Double",
        "Collections": [
            "SENTINEL-3",
        ],
    },
    "collectionCategory": {
        "Type": "String",
        "Collections": [
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "collectionName": {
        "Type": "String",
        "Collections": [
            "LANDSAT-5",
            "LANDSAT-7",
        ],
    },
    "collectionNumber": {
        "Type": "String",
        "Collections": [
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "completionTimeFromAscendingNode": {
        "Type": "Double",
        "Collections": [
            "SENTINEL-1",
        ],
    },
    "continentalIceCover": {
        "Type": "Integer",
        "Collections": [
            "SENTINEL-3",
        ],
    },
    "coordinates": {
        "Type": "String",
        "Collections": [
            "SENTINEL-1",
            "SENTINEL-2",
            "SENTINEL-3",
            "SENTINEL-5P",
        ],
    },
    "cycleNumber": {
        "Type": "Integer",
        "Collections": [
            "SENTINEL-1",
            "SENTINEL-3",
            "ENVISAT",
        ],
    },
    "datastripId": {
        "Type": "String",
        "Collections": [
            "SENTINEL-2",
        ],
    },
    "datatakeID": {
        "Type": "Integer",
        "Collections": [
            "SENTINEL-1",
        ],
    },
    "doi": {
        "Type": "String",
        "Collections": [
            "SENTINEL-5P",
        ],
    },
    "freshInlandWaterCover": {
        "Type": "Double",
        "Collections": [
            "SENTINEL-3",
        ],
    },
    "geometricRmse": {
        "Type": "Double",
        "Collections": [
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "geometricXBias": {
        "Type": "Double",
        "Collections": [
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "geometricXStddev": {
        "Type": "Double",
        "Collections": [
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "geometricYBias": {
        "Type": "Double",
        "Collections": [
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "geometricYStddev": {
        "Type": "Double",
        "Collections": [
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "granuleIdentifier": {
        "Type": "String",
        "Collections": [
            "SENTINEL-2",
        ],
    },
    "identifier": {
        "Type": "String",
        "Collections": [
            "SENTINEL-5P",
        ],
    },
    "illuminationZenithAngle": {
        "Type": "Double",
        "Collections": [
            "SENTINEL-2",
        ],
    },
    "instrumentConfigurationID": {
        "Type": "Integer",
        "Collections": [
            "SENTINEL-1",
        ],
    },
    "instrumentShortName": {
        "Type": "String",
        "Collections": [
            "SENTINEL-1",
            "SENTINEL-2",
            "SENTINEL-3",
            "SENTINEL-5P",
            "SENTINEL-6",
            "SENTINEL-1-RTC",
            "ENVISAT",
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "landCover": {
        "Type": "Double",
        "Collections": [
            "SENTINEL-3",
        ],
    },
    "lastOrbitDirection": {
        "Type": "String",
        "Collections": [
            "SENTINEL-3",
        ],
    },
    "lastOrbitNumber": {
        "Type": "Integer",
        "Collections": [
            "SENTINEL-2",
            "SENTINEL-3",
        ],
    },
    "lastRelativeOrbitNumber": {
        "Type": "Integer",
        "Collections": [
            "SENTINEL-3",
        ],
    },
    "numberOfBands": {
        "Type": "Integer",
        "Collections": [
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "offNadir": {
        "Type": "Double",
        "Collections": [
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "openOceanCover": {
        "Type": "Integer",
        "Collections": [
            "SENTINEL-3",
        ],
    },
    "operationalMode": {
        "Type": "String",
        "Collections": [
            "SENTINEL-1",
            "SENTINEL-2",
            "SENTINEL-3",
            "SENTINEL-6",
            "SENTINEL-1-RTC",
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "orbitDirection": {
        "Type": "String",
        "Title": "Orbit direction (ASCENDING or DESCENDING)",
        "Collections": [
            "SENTINEL-1",
            "SENTINEL-3",
            "SENTINEL-1-RTC",
        ],
    },
    "orbitNumber": {
        "Type": "Integer",
        "Title": "Absolute orbit number",
        "Collections": [
            "SENTINEL-1",
            "SENTINEL-2",
            "SENTINEL-3",
            "SENTINEL-5P",
            "SENTINEL-6",
            "SENTINEL-1-RTC",
            "ENVISAT",
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "origin": {
        "Type": "String",
        "Collections": [
            "SENTINEL-1",
            "SENTINEL-2",
            "SENTINEL-6",
        ],
    },
    "parentIdentifier": {
        "Type": "String",
        "Collections": [
            "SENTINEL-5P",
        ],
    },
    "pathNumber": {
        "Type": "Integer",
        "Collections": [
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "phaseNumber": {
        "Type": "Integer",
        "Collections": [
            "ENVISAT",
        ],
    },
    "platformSerialIdentifier": {
        "Type": "String",
        "Collections": [
            "SENTINEL-1",
            "SENTINEL-2",
            "SENTINEL-3",
            "SENTINEL-5P",
            "SENTINEL-6",
            "SENTINEL-1-RTC",
        ],
    },
    "platformShortName": {
        "Type": "String",
        "Collections": [
            "SENTINEL-1",
            "SENTINEL-2",
            "SENTINEL-3",
            "SENTINEL-5P",
            "SENTINEL-6",
            "SENTINEL-1-RTC",
            "ENVISAT",
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "polarisationChannels": {
        "Type": "String",
        "Collections": [
            "SENTINEL-1",
            "SENTINEL-1-RTC",
        ],
    },
    "processingBaseline": {
        "Type": "String",
        "Collections": [
            "SENTINEL-1",
            "SENTINEL-2",
            "SENTINEL-3",
            "SENTINEL-5P",
            "SENTINEL-6",
        ],
    },
    "processingCenter": {
        "Type": "String",
        "Collections": [
            "SENTINEL-1",
            "SENTINEL-2",
            "SENTINEL-3",
            "SENTINEL-5P",
            "SENTINEL-6",
        ],
    },
    "processingDate": {
        "Type": "DateTimeOffset",
        "Collections": [
            "SENTINEL-1",
            "SENTINEL-2",
            "SENTINEL-3",
            "SENTINEL-5P",
            "SENTINEL-6",
        ],
    },
    "processingLevel": {
        "Type": "String",
        "Title": "Processing level",
        "Collections": [
            "SENTINEL-1",
            "SENTINEL-2",
            "SENTINEL-3",
            "SENTINEL-5P",
            "SENTINEL-6",
            "SENTINEL-1-RTC",
            "ENVISAT",
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "processingMode": {
        "Type": "String",
        "Collections": [
            "SENTINEL-5P",
        ],
    },
    "processorName": {
        "Type": "String",
        "Collections": [
            "SENTINEL-1",
            "SENTINEL-3",
            "SENTINEL-5P",
        ],
    },
    "processorVersion": {
        "Type": "String",
        "Collections": [
            "SENTINEL-1",
            "SENTINEL-2",
            "SENTINEL-3",
            "SENTINEL-5P",
            "SENTINEL-6",
        ],
    },
    "productClass": {
        "Type": "String",
        "Collections": [
            "SENTINEL-1",
            "SENTINEL-5P",
        ],
    },
    "productComposition": {
        "Type": "String",
        "Collections": [
            "SENTINEL-1",
        ],
    },
    "productConsolidation": {
        "Type": "String",
        "Collections": [
            "SENTINEL-1",
        ],
    },
    "productGeneration": {
        "Type": "DateTimeOffset",
        "Collections": [
            "SENTINEL-1",
        ],
    },
    "productGroupId": {
        "Type": "String",
        "Collections": [
            "SENTINEL-2",
        ],
    },
    "productType": {
        "Type": "String",
        "Title": "Product type",
        "Collections": [
            "SENTINEL-1",
            "SENTINEL-2",
            "SENTINEL-3",
            "SENTINEL-5P",
            "SENTINEL-6",
            "SENTINEL-1-RTC",
            "ENVISAT",
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "proj:epsg": {
        "Type": "Integer",
        "Collections": [
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "projShape": {
        "Type": "String",
        "Collections": [
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "projTransform": {
        "Type": "String",
        "Collections": [
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "qualityInfo": {
        "Type": "Integer",
        "Collections": [
            "SENTINEL-2",
        ],
    },
    "qualityStatus": {
        "Type": "String",
        "Collections": [
            "SENTINEL-2",
            "SENTINEL-5P",
        ],
    },
    "relativeOrbitNumber": {
        "Type": "Integer",
        "Title": "Relative orbit number",
        "Collections": [
            "SENTINEL-1",
            "SENTINEL-2",
            "SENTINEL-3",
            "SENTINEL-6",
            "SENTINEL-1-RTC",
        ],
    },
    "rowNumber": {
        "Type": "Integer",
        "Collections": [
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "salineWaterCover": {
        "Type": "Double",
        "Collections": [
            "SENTINEL-3",
        ],
    },
    "sceneId": {
        "Type": "String",
        "Collections": [
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "segmentStartTime": {
        "Type": "DateTimeOffset",
        "Collections": [
            "SENTINEL-1",
        ],
    },
    "sliceNumber": {
        "Type": "Integer",
        "Collections": [
            "SENTINEL-1",
        ],
    },
    "sliceProductFlag": {
        "Type": "Boolean",
        "Collections": [
            "SENTINEL-1",
        ],
    },
    "snowOrIceCover": {
        "Type": "Double",
        "Collections": [
            "SENTINEL-3",
        ],
    },
    "source": {
        "Type": "String",
        "Collections": [
            "SENTINEL-6",
        ],
    },
    "sourceProduct": {
        "Type": "String",
        "Collections": [
            "SENTINEL-2",
        ],
    },
    "sourceProductOriginDate": {
        "Type": "String",
        "Collections": [
            "SENTINEL-2",
        ],
    },
    "spatialResolution": {
        "Type": "Integer",
        "Collections": [
            "SENTINEL-6",
            "SENTINEL-1-RTC",
            "ENVISAT",
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "startTimeFromAscendingNode": {
        "Type": "Double",
        "Collections": [
            "SENTINEL-1",
        ],
    },
    "sunAzimuthAngle": {
        "Type": "Double",
        "Collections": [
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "sunElevationAngle": {
        "Type": "Double",
        "Collections": [
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "swathIdentifier": {
        "Type": "String",
        "Collections": [
            "SENTINEL-1",
        ],
    },
    "tidalRegionCover": {
        "Type": "Double",
        "Collections": [
            "SENTINEL-3",
        ],
    },
    "tileId": {
        "Type": "String",
        "Collections": [
            "SENTINEL-2",
        ],
    },
    "timeliness": {
        "Type": "String",
        "Collections": [
            "SENTINEL-1",
            "SENTINEL-3",
            "SENTINEL-6",
        ],
    },
    "totalSlices": {
        "Type": "Integer",
        "Collections": [
            "SENTINEL-1",
        ],
    },
    "view:sun_azimuth": {
        "Type": "Double",
        "Collections": [
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "view:sun_elevation": {
        "Type": "Double",
        "Collections": [
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "wrsPath": {
        "Type": "String",
        "Collections": [
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "wrsRow": {
        "Type": "String",
        "Collections": [
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
    "wrsType": {
        "Type": "String",
        "Collections": [
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "LANDSAT-9",
        ],
    },
}
