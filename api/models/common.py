#!/usr/bin/env python3

from enum import Enum


Coordinate = tuple[float, float]
RouteInCoordinate = list[Coordinate]

ISOName = str
RouteInISO = list[ISOName]


class CarbonDataSource(str, Enum):
    C3Lab = "c3lab"
    Azure = "azure"
    EMap = "electricity-map"


class IsoFormat(str, Enum):
    C3Lab = "c3lab"
    WattTime = "watttime"
    EMap = "electricity-map"
    Unknown = "unknown"


ISO_PREFIX_WATTTIME = f'{IsoFormat.WattTime}:'
ISO_PREFIX_C3LAB = f'{IsoFormat.C3Lab}:'
ISO_PREFIX_EMAP = f'{IsoFormat.EMap}:'


def identify_iso_format(iso: str) -> IsoFormat:
    for prefix in IsoFormat:
        if iso.startswith(prefix):
            return prefix
    return IsoFormat.Unknown

def get_iso_format_for_carbon_source(carbon_data_source: CarbonDataSource) -> IsoFormat:
    if carbon_data_source == CarbonDataSource.C3Lab:
        return IsoFormat.WattTime
    elif carbon_data_source == CarbonDataSource.Azure:
        return IsoFormat.WattTime
    elif carbon_data_source == CarbonDataSource.EMap:
        return IsoFormat.EMap
    else:
        raise ValueError(f"Unknown carbon_data_source: {carbon_data_source}")
