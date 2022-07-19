#!/usr/bin/env python3

from dataclasses import dataclass
from enum import Enum
import os
from pathlib import Path
from typing import Tuple

from api.util import loadYamlData, logger

class CloudLocationLookupException(Exception):
    pass

@dataclass
class CloudRegion:
    provider: str
    code: str
    name: str
    gps: Tuple[float, float]

@dataclass
class PublicCloud:
    provider: str
    regions: list[CloudRegion]

class CloudLocationManager:
    all_public_clouds: dict[str, PublicCloud] = {}

    def __init__(self) -> None:
        config_path = os.path.join(Path(__file__).parent.absolute(), 'cloud_location.yaml')
        yaml_data = loadYamlData(config_path)
        public_cloud_list_name = 'public_clouds'
        assert yaml_data is not None and public_cloud_list_name in yaml_data, \
            f'Failed to load {public_cloud_list_name}'
        l_raw_public_clouds = yaml_data[public_cloud_list_name]
        for raw_public_cloud in l_raw_public_clouds:
            cloud_provider = raw_public_cloud['provider']
            l_raw_cloud_regions = raw_public_cloud['regions']
            l_cloud_regions: list[CloudRegion] = []
            for raw_cloud_region in l_raw_cloud_regions:
                region_code = raw_cloud_region['code']
                region_name = raw_cloud_region['name']
                region_gps = tuple([float(coordinate) for coordinate in raw_cloud_region['gps']])
                assert len(region_gps) == 2 and abs(region_gps[0]) <= 90 and abs(region_gps[1]) <= 180, \
                    f"Invalid GPS coordinate {region_gps} for {cloud_provider}:{region_code}"
                new_cloud_region = CloudRegion(cloud_provider, region_code, region_name, region_gps)
                l_cloud_regions.append(new_cloud_region)
            new_public_cloud = PublicCloud(cloud_provider, l_cloud_regions)
            self.all_public_clouds[cloud_provider] = new_public_cloud

    def get_all_clouds_by_provider(self) -> dict[str, PublicCloud]:
        return self.all_public_clouds

    def get_all_cloud_providers(self) -> list[str]:
        return sorted(self.all_public_clouds.keys())

    def get_cloud_region_codes(self, cloud_provider: str) -> list[str]:
        logger.debug('get_cloud_region_codes(%s)' % cloud_provider)
        if cloud_provider not in self.all_public_clouds:
            return []
        return [region.code for region in self.all_public_clouds[cloud_provider].regions]

    def get_gps_coordinate(self, cloud_provider: str, region_code: str) -> Tuple[float, float]:
        if cloud_provider not in self.all_public_clouds:
            raise CloudLocationLookupException('Unknown cloud provider "%s".' % cloud_provider)
        for region in self.all_public_clouds[cloud_provider].regions:
            if region.code == region_code:
                return region.gps
        raise CloudLocationLookupException('Unknown region "%s" for provider "%s".' % (region_code, cloud_provider))
