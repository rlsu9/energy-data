#!/usr/bin/env python3
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any

import marshmallow_dataclass
from flask import current_app
from flask_restful import Resource
from webargs.flaskparser import use_args

from api.helpers.carbon_intensity import convert_carbon_intensity_list_to_dict, calculate_total_carbon_emissions, \
    get_carbon_intensity_list
from api.models.cloud_location import CloudLocationManager, CloudRegion
from api.models.optimization_engine import OptimizationEngine, OptimizationFactor
from api.models.wan_bandwidth import load_wan_bandwidth_model
from api.models.workload import DEFAULT_CPU_POWER_PER_CORE, CloudLocation, Workload
from api.routes.balancing_authority import convert_watttime_ba_abbrev_to_region, lookup_watttime_balancing_authority
from api.util import PSqlExecuteException, SizeUnit, Size, Rate

g_cloud_manager = CloudLocationManager()
OPTIMIZATION_FACTORS_AND_WEIGHTS = [
    (OptimizationFactor.EnergyUsage, 1000),
    (OptimizationFactor.CarbonEmission, 1000),
    (OptimizationFactor.WanNetworkUsage, 0.001),
]
g_optimizer = OptimizationEngine([t[0] for t in OPTIMIZATION_FACTORS_AND_WEIGHTS],
                                 [t[1] for t in OPTIMIZATION_FACTORS_AND_WEIGHTS])
g_wan_bandwidth = load_wan_bandwidth_model()


def get_candidate_regions(candidate_providers: list[str], candidate_locations: list[CloudLocation]) \
        -> list[CloudRegion]:
    try:
        if candidate_providers:
            return g_cloud_manager.get_all_cloud_regions(candidate_providers)
        candidate_regions = []
        for location in candidate_locations:
            (provider, region_name) = location.id.split(':', 1)
            cloud_region = CloudRegion(provider, region_name, location.id, None, (location.latitude, location.longitude))
            candidate_regions.append(cloud_region)
        return candidate_regions
    except Exception as ex:
        raise ValueError(f'Failed to get candidate regions: {ex}') from ex


def lookup_iso_region(gps: tuple[float, float]):
    try:
        (latitude, longitude) = gps
        watttime_lookup_result = lookup_watttime_balancing_authority(latitude, longitude)
        return convert_watttime_ba_abbrev_to_region(watttime_lookup_result['watttime_abbrev'])
    except Exception as ex:
        raise ValueError(f'Failed to lookup ISO region: {ex}') from ex

def calculate_workload_scores(workload: Workload, cloud_region: CloudRegion, iso_region: str) ->\
        tuple[dict[OptimizationFactor, float], dict[str, Any]]:
    d_scores = {}
    d_misc = {}
    for factor in OptimizationFactor:
        match factor:
            case OptimizationFactor.EnergyUsage:
                # score = per-core power (kW) * cpu usage (h)
                score = workload.get_energy_usage_24h()
            case OptimizationFactor.CarbonEmissionFromCompute: continue
            case OptimizationFactor.CarbonEmissionFromMigration: continue
            case OptimizationFactor.CarbonEmission:
                # score = energy usage (kWh) * grid carbon intensity (kgCO2/kWh)
                running_intervals = workload.get_running_intervals_in_24h()
                max_delay = workload.schedule.max_delay
                score = 0
                d_misc['start_delay'] = []
                d_misc['migration_emission'] = []
                d_misc['migration_duration'] = []
                # 24 hour / 5 min = 288 slots
                for (start, end) in running_intervals:
                    l_carbon_intensity = get_carbon_intensity_list(iso_region, start, end + max_delay)
                    carbon_intensity_by_timestamp = convert_carbon_intensity_list_to_dict(l_carbon_intensity)
                    total_compute_carbon_emissions, optimal_delay_time = calculate_total_carbon_emissions(
                        start, end, DEFAULT_CPU_POWER_PER_CORE, carbon_intensity_by_timestamp, max_delay)
                    d_misc['start_delay'].append(optimal_delay_time)
                    # Note: temporarily disabled
                    """
                    # Migration emission
                    # migration carbon emission = min[t](carbon intensity * migration power * migration duration)
                    #   constraint(t):  t(migration out) <= t(execution start) < t(execution end) <= t(migration back)
                    #       e.g. migration window directly bounding execution time window => possibly same optimization?
                    #   migration power = 20% * normal energy
                    #       (from Barroso book: 20% because the majority of energy of a DC is on CPU/compute)
                    #   migration duration (out/back) = data size / available bandwidth per workload
                    input_size = Size(workload.dataset.input_size_gb, SizeUnit.GB)
                    output_size = Size(workload.dataset.output_size_gb, SizeUnit.GB)
                    available_bandwidth_start: Rate = g_wan_bandwidth.available_bandwidth_at(timestamp=start.time())
                    pre_run_migration_duration: timedelta = input_size / available_bandwidth_start
                    available_bandwidth_end: Rate = g_wan_bandwidth.available_bandwidth_at(timestamp=end.time())
                    post_run_migration_duration: timedelta = output_size / available_bandwidth_end
                    pre_run_migration_carbon_emission, _ = calculate_total_carbon_emissions(
                        start,
                        start + pre_run_migration_duration,
                        DEFAULT_STORAGE_POWER,
                        carbon_intensity_by_timestamp
                    )
                    post_run_migration_carbon_emission, _ = calculate_total_carbon_emissions(
                        end,
                        end + post_run_migration_duration,
                        DEFAULT_STORAGE_POWER,
                        carbon_intensity_by_timestamp
                    )
                    total_migration_carbon_emission = pre_run_migration_carbon_emission + post_run_migration_carbon_emission
                    d_misc['migration_emission'].append((pre_run_migration_carbon_emission, post_run_migration_carbon_emission))
                    d_misc['migration_duration'].append((pre_run_migration_duration, post_run_migration_duration))
                    """
                    total_migration_carbon_emission = 0
                    d_scores[OptimizationFactor.CarbonEmissionFromCompute] = total_compute_carbon_emissions
                    d_scores[OptimizationFactor.CarbonEmissionFromMigration] = total_migration_carbon_emission
                    score += (total_compute_carbon_emissions + total_migration_carbon_emission)
            case OptimizationFactor.WanNetworkUsage:
                # score = input + output data size (GB)
                # TODO: add WAN demand as weight
                score = workload.dataset.input_size_gb + workload.dataset.output_size_gb
            case _:  # Other factors ignored
                score = 0
                continue
        d_scores[factor] = score
    return d_scores, d_misc


class CarbonAwareScheduler(Resource):
    @use_args(marshmallow_dataclass.class_schema(Workload)())
    def get(self, args: Workload):
        workload = args
        orig_request = {'request': workload}
        current_app.logger.info("CarbonAwareScheduler.get(%s)" % workload)

        # TODO: use prediction data instead of historic data
        if workload.schedule.start_time is None:
            workload.schedule.start_time = datetime.now(timezone.utc) + timedelta(days=-1)

        # candidate_cloud_regions = get_alternative_regions(args.original_location, True)
        candidate_cloud_regions = get_candidate_regions(args.candidate_providers, args.candidate_locations)
        for candidate_cloud_region in candidate_cloud_regions:
            if not candidate_cloud_region.iso:
                candidate_cloud_region.iso = lookup_iso_region(candidate_cloud_region.gps)
        l_region_scores = []
        d_region_warnings = dict()
        d_misc_details = dict()
        for cloud_region in candidate_cloud_regions:
            try:
                workload_scores, d_misc = calculate_workload_scores(workload, cloud_region, cloud_region.iso)
                # if all([delay > timedelta() for delay in d_misc['start_delay']]):
                d_misc_details[str(cloud_region)] = d_misc
                l_region_scores.append(workload_scores)
            # except PSqlExecuteException as e:
            #     raise e
            except Exception as e:
                d_region_warnings[str(cloud_region)] = str(e)
                current_app.logger.error(f'Exception when calculating score for region {cloud_region}: {e}')
                current_app.logger.error(traceback.format_exc())
                l_region_scores.append({})
        index_best_region, l_weighted_score = g_optimizer.compare_candidates(l_region_scores, True)
        if index_best_region == -1:
            return {
                'error': 'No viable candidate',
                'details': d_region_warnings
            }, 400
        selected_region = str(candidate_cloud_regions[index_best_region])

        d_weighted_scores = {}
        d_raw_scores = {}
        d_cloud_region_to_iso = {}
        for i in range(len(candidate_cloud_regions)):
            region_name = str(candidate_cloud_regions[i])
            d_weighted_scores[region_name] = l_weighted_score[i]
            d_raw_scores[region_name] = l_region_scores[i]
            d_cloud_region_to_iso[region_name] = candidate_cloud_regions[i].iso
        return orig_request | {
            'original-region': str(args.original_location),
            'selected-region': selected_region,
            'isos': d_cloud_region_to_iso,
            'weighted-scores': d_weighted_scores,
            'raw-scores': d_raw_scores,
            'warnings': d_region_warnings,
            'details': d_misc_details,
        }
