#!/usr/bin/env python3

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Tuple
from flask import current_app
from werkzeug.exceptions import NotFound

from api.util import load_yaml_data


@dataclass
class CloudRegion:
    provider: str
    code: str
    name: str
    iso: str
    gps: Tuple[float, float]

    def __str__(self) -> str:
        return f'{self.provider}:{self.code}'


@dataclass
class PublicCloud:
    provider: str
    regions: list[CloudRegion]


class CloudLocationManager:
    all_public_clouds: dict[str, PublicCloud] = {}

    def __init__(self) -> None:
        config_path = os.path.join(Path(__file__).parent.absolute(), 'cloud_location.yaml')
        yaml_data = load_yaml_data(config_path)
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
                region_iso = raw_cloud_region['iso']
                region_gps = tuple([float(coordinate) for coordinate in raw_cloud_region['gps']])
                assert len(region_gps) == 2 and abs(region_gps[0]) <= 90 and abs(region_gps[1]) <= 180, \
                    f"Invalid GPS coordinate {region_gps} for {cloud_provider}:{region_code}"
                new_cloud_region = CloudRegion(cloud_provider, region_code, region_name, region_iso, region_gps)
                l_cloud_regions.append(new_cloud_region)
            new_public_cloud = PublicCloud(cloud_provider, l_cloud_regions)
            self.all_public_clouds[cloud_provider] = new_public_cloud

    def get_all_clouds_by_provider(self) -> dict[str, PublicCloud]:
        return self.all_public_clouds

    def get_all_cloud_providers(self) -> list[str]:
        return sorted(self.all_public_clouds.keys())

    def get_all_cloud_regions(self) -> list[CloudRegion]:
        all_cloud_regions: list[CloudRegion] = []
        for public_cloud in self.all_public_clouds.values():
            all_cloud_regions += public_cloud.regions
        return all_cloud_regions

    def get_cloud_region_codes(self, cloud_provider: str) -> list[str]:
        current_app.logger.debug('get_cloud_region_codes(%s)' % cloud_provider)
        if cloud_provider not in self.all_public_clouds:
            return []
        return [region.code for region in self.all_public_clouds[cloud_provider].regions]

    def get_gps_coordinate(self, cloud_region: CloudRegion = None, cloud_provider: str = None,
                           region_code: str = None) -> Tuple[float, float]:
        if not cloud_provider and not region_code and cloud_region:
            cloud_provider = cloud_region.provider
            region_code = cloud_region.code
        if cloud_provider not in self.all_public_clouds:
            raise NotFound('Unknown cloud provider "%s".' % cloud_provider)
        for region in self.all_public_clouds[cloud_provider].regions:
            if region.code == region_code:
                return region.gps
        raise NotFound('Unknown region "%s" for provider "%s".' % (region_code, cloud_provider))
