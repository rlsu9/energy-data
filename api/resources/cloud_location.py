#!/usr/bin/env python3

from dataclasses import dataclass
from enum import Enum
import os
from pathlib import Path
from typing import Tuple

from api.util import loadYamlData

class CloudProvider(Enum):
    AWS = 'AWS'

@dataclass
class CloudRegion:
    provider: str
    code: str
    name: str
    gps: Tuple[float, float]

@dataclass
class PublicCloud:
    provider: CloudProvider
    regions: list[CloudRegion]

class CloudLocationManager:
    @classmethod
    def get_all_cloud_locations(self) -> dict[str, list[PublicCloud]]:
        config_path = os.path.join(Path(__file__).parent.absolute(), 'cloud_location.yaml')
        yaml_data = loadYamlData(config_path)
        public_cloud_list_name = 'public_clouds'
        assert yaml_data is not None and public_cloud_list_name in yaml_data, \
            f'Failed to load {public_cloud_list_name}'
        l_raw_public_clouds = yaml_data[public_cloud_list_name]
        l_public_clouds = []
        for raw_public_cloud in l_raw_public_clouds:
            cloud_provider = raw_public_cloud['provider']
            l_raw_cloud_regions = raw_public_cloud['regions']
            l_cloud_regions = []
            for raw_cloud_region in l_raw_cloud_regions:
                region_code = raw_cloud_region['code']
                region_name = raw_cloud_region['name']
                region_gps = tuple([float(coordinate) for coordinate in raw_cloud_region['gps']])
                assert len(region_gps) == 2 and abs(region_gps[0]) <= 90 and abs(region_gps[1]) <= 180, \
                    f"Invalid GPS coordinate {region_gps} for {cloud_provider}:{region_code}"
                new_cloud_region = CloudRegion(cloud_provider, region_code, region_name, region_gps)
                l_cloud_regions.append(new_cloud_region)
            new_public_cloud = PublicCloud(cloud_provider, l_cloud_regions)
            l_public_clouds.append(new_public_cloud)
        return l_public_clouds
