#!/usr/bin/env python3

from datetime import datetime, timedelta, timezone
from flask_restful import Resource
from typing import Any
from webargs.flaskparser import use_args
import marshmallow_dataclass
from flask import current_app

from api.helpers.carbon_intensity import convert_carbon_intensity_list_to_dict, calculate_total_carbon_emissions, \
    get_carbon_intensity_list
from api.routes.balancing_authority import convert_watttime_ba_abbrev_to_region, lookup_watttime_balancing_authority
from api.models.cloud_location import CloudLocationManager, CloudRegion
from api.models.workload import DEFAULT_CPU_POWER_PER_CORE, Workload
from api.models.optimization_engine import OptimizationEngine, OptimizationFactor

g_cloud_manager = CloudLocationManager()
OPTIMIZATION_FACTORS_AND_WEIGHTS = [
    (OptimizationFactor.EnergyUsage, 1000),
    (OptimizationFactor.CarbonIntensity, 1000),
    (OptimizationFactor.WanNetworkUsage, 0.001),
]
g_optimizer = OptimizationEngine([t[0] for t in OPTIMIZATION_FACTORS_AND_WEIGHTS],
                                 [t[1] for t in OPTIMIZATION_FACTORS_AND_WEIGHTS])


def get_alternative_regions(cloud_region: CloudRegion = None, include_self=False) -> list[CloudRegion]:
    # NOTE: returns all possible regions for now, but can add filter/preference later.
    return g_cloud_manager.get_all_cloud_regions()


def calculate_workload_scores(workload: Workload, cloud_region: CloudRegion, iso_region: str) ->\
        tuple[dict[OptimizationFactor, float], dict[str, Any]]:
    d_scores = {}
    d_misc = {}
    for factor in OptimizationFactor:
        match factor:
            case OptimizationFactor.EnergyUsage:
                # score = per-core power (kW) * cpu usage (h)
                score = workload.get_energy_usage_24h()
            case OptimizationFactor.CarbonIntensity:
                # score = energy usage (kWh) * grid carbon intensity (kgCO2/kWh)
                running_intervals = workload.get_running_intervals_in_24h()
                max_delay = workload.schedule.max_delay
                score = 0
                d_misc['optimal_delay_time'] = []
                for (start, end) in running_intervals:
                    l_carbon_intensity = get_carbon_intensity_list(iso_region, start, end + max_delay)
                    carbon_intensity_by_timestamp = convert_carbon_intensity_list_to_dict(l_carbon_intensity)
                    total_carbon_emissions, optimal_delay_time = calculate_total_carbon_emissions(
                        start, end, DEFAULT_CPU_POWER_PER_CORE, carbon_intensity_by_timestamp, max_delay)
                    # TODO: pass optimal_start_time to scheduler
                    score += total_carbon_emissions
                    d_misc['optimal_delay_time'].append(optimal_delay_time)
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
        if workload.schedule.start_time is None:
            workload.schedule.start_time = datetime.now(timezone.utc)

        # TODO: use prediction data instead of historic data
        workload.schedule.start_time -= timedelta(days=1)

        candidate_cloud_regions = get_alternative_regions(args.preferred_cloud_location, True)
        candidate_iso_regions = []
        for candidate_cloud_region in candidate_cloud_regions:
            (latitude, longitude) = g_cloud_manager.get_gps_coordinate(candidate_cloud_region)
            watttime_lookup_result = lookup_watttime_balancing_authority(latitude, longitude)
            iso_region = convert_watttime_ba_abbrev_to_region(watttime_lookup_result['watttime_abbrev'])
            candidate_iso_regions.append(iso_region)
        l_region_scores = []
        l_region_names = []
        d_region_warnings = dict()
        d_region_delay = dict()
        for i in range(len(candidate_cloud_regions)):
            cloud_region = candidate_cloud_regions[i]
            iso_region = candidate_iso_regions[i]
            try:
                workload_scores, d_misc = calculate_workload_scores(workload, cloud_region, iso_region)
                # if all([delay > timedelta() for delay in d_misc['optimal_delay_time']]):
                d_region_delay[str(cloud_region)] = d_misc['optimal_delay_time']
                l_region_scores.append(workload_scores)
                l_region_names.append(str(cloud_region))
            except Exception as e:
                d_region_warnings[str(cloud_region)] = str(e)
        index_best_region, l_weighted_score = g_optimizer.compare_candidates(l_region_scores, True)
        selected_region = l_region_names[index_best_region]

        d_weighted_scores = {l_region_names[i]: l_weighted_score[i] for i in range(len(l_region_names))}
        d_raw_scores = {l_region_names[i]: l_region_scores[i] for i in range(len(l_region_names))}
        d_cloud_region_to_iso = {str(candidate_cloud_regions[i]): candidate_iso_regions[i]
                                 for i in range(len(candidate_cloud_regions))}
        return orig_request | {
            'requested-region': str(args.preferred_cloud_location),
            'selected-region': selected_region,
            'iso': d_cloud_region_to_iso,
            'weighted-scores': d_weighted_scores,
            'raw-scores': d_raw_scores,
            'warnings': d_region_warnings,
            'start_delay': d_region_delay,
        }
