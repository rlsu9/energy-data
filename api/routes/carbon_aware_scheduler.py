#!/usr/bin/env python3
from datetime import timedelta, timezone
from multiprocessing import Pool
import traceback
from typing import Any

import marshmallow_dataclass
from flask import current_app
from flask_restful import Resource
from webargs.flaskparser import use_args

from api.helpers.carbon_intensity import CarbonDataSource, calculate_total_carbon_emissions, convert_carbon_intensity_list_to_dict, get_carbon_intensity_list
from api.models.cloud_location import CloudLocationManager, CloudRegion
from api.models.optimization_engine import OptimizationEngine, OptimizationFactor
from api.models.wan_bandwidth import load_wan_bandwidth_model
from api.models.workload import CloudLocation, Workload
from api.models.dataclass_extensions import *
from api.routes.balancing_authority import lookup_watttime_balancing_authority
from api.util import round_up

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
            if location.latitude and location.longitude:
                gps = (location.latitude, location.longitude)
                cloud_region = CloudRegion(provider, region_name, location.id, None, gps)
            else:
                cloud_region = g_cloud_manager.get_cloud_region(provider, region_name)
            candidate_regions.append(cloud_region)
        return candidate_regions
    except Exception as ex:
        raise ValueError(f'Failed to get candidate regions: {ex}') from ex

def lookup_iso_region(gps: tuple[float, float]):
    try:
        (latitude, longitude) = gps
        watttime_lookup_result = lookup_watttime_balancing_authority(latitude, longitude)
        return watttime_lookup_result['watttime_abbrev']
    except Exception as ex:
        raise ValueError(f'Failed to lookup ISO region: {ex}') from ex

def task_lookup_iso(region: CloudRegion) -> tuple:
    if region.iso:
        return str(region), region.iso, None, None
    try:
        iso = lookup_iso_region(region.gps)
        return str(region), iso, None, None
    except Exception as ex:
        return str(region), None, str(ex), traceback.format_exc()

def init_preload_carbon_data(_workload: Workload,
                                    _carbon_data_source: CarbonDataSource,
                                    _use_prediction: bool,
                                    _desired_renewable_ratio: float = None):
    global workload, carbon_data_source, use_prediction, desired_renewable_ratio
    workload = _workload
    carbon_data_source = _carbon_data_source
    use_prediction = _use_prediction
    desired_renewable_ratio = _desired_renewable_ratio

def preload_carbon_data(workload: Workload,
                        iso: str,
                        carbon_data_source: CarbonDataSource,
                        use_prediction: bool,
                        desired_renewable_ratio: float = None):
    carbon_data_store = dict()
    running_intervals = workload.get_running_intervals_in_24h()
    for (start, end) in running_intervals:
        max_delay = workload.schedule.max_delay
        carbon_data_store[(iso, start, end)] = get_carbon_intensity_list(iso, start, end + max_delay,
                                                        carbon_data_source, use_prediction,
                                                        desired_renewable_ratio)
    return carbon_data_store

def task_preload_carbon_data(iso: str) -> tuple:
    global workload, carbon_data_source, use_prediction
    try:
        carbon_data = preload_carbon_data(workload, iso, carbon_data_source, use_prediction)
        return iso, carbon_data, None, None
    except Exception as ex:
        return iso, None, str(ex), traceback.format_exc()

def init_parallel_process_candidate(_workload: Workload,
                                    _carbon_data_source: CarbonDataSource,
                                    _use_prediction: bool,
                                    _carbon_data_store: dict):
    global workload, carbon_data_source, use_prediction, carbon_data_store
    workload = _workload
    carbon_data_source = _carbon_data_source
    use_prediction = _use_prediction
    carbon_data_store = _carbon_data_store

def get_preloaded_carbon_data(iso: str, start: datetime, end: datetime) -> list[dict]:
    global carbon_data_store
    key = (iso, start, end)
    if key in carbon_data_store:
        return carbon_data_store[key]
    else:
        raise ValueError(f'No carbon data found for iso {iso} in time range ({start}, {end})')

def calculate_workload_scores(workload: Workload, iso: str) -> tuple[dict[OptimizationFactor, float], dict[str, Any]]:
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
                    l_carbon_intensity = get_preloaded_carbon_data(iso, start, end)
                    carbon_intensity_by_timestamp = convert_carbon_intensity_list_to_dict(l_carbon_intensity)
                    total_compute_carbon_emissions, optimal_delay_time = calculate_total_carbon_emissions(
                        start, end, workload.get_power_in_watts(), carbon_intensity_by_timestamp, max_delay)
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

def task_process_candidate(region: CloudRegion) -> tuple:
    global workload, carbon_data_source, use_prediction
    region_name = str(region)
    iso = region.iso
    try:
        scores, d_misc = calculate_workload_scores(workload, iso)
        return region_name, iso, scores, d_misc, None, None
    except Exception as ex:
        return region_name, iso, None, None, str(ex), traceback.format_exc()


class CarbonAwareScheduler(Resource):
    @use_args(marshmallow_dataclass.class_schema(Workload)())
    def get(self, args: Workload):
        workload = args
        orig_request = {'request': workload}
        current_app.logger.info("CarbonAwareScheduler.get(%s)" % workload)

        if workload.use_prediction:
            min_start_time = round_up(datetime.now(timezone.utc), timedelta(minutes=5))
            if workload.schedule.start_time < min_start_time:
                workload.schedule.start_time = min_start_time

        candidate_regions = get_candidate_regions(args.candidate_providers, args.candidate_locations)

        d_region_isos = dict()
        d_region_scores = dict()
        d_region_warnings = dict()
        d_misc_details = dict()

        with Pool(4) as pool:
            result_iso = pool.map(task_lookup_iso, candidate_regions)
        for i in range(len(candidate_regions)):
            (region_name, iso, ex, stack_trace) = result_iso[i]
            if iso:
                d_region_isos[region_name] = iso
                candidate_regions[i].iso = iso
            else:
                d_region_warnings[region_name] = ex
                current_app.logger.error(f'ISO lookup failed for {region_name}: {ex}')
                current_app.logger.error(stack_trace)

        unique_isos = set(d_region_isos.values())
        carbon_data = dict()
        d_iso_errors = dict()
        with Pool(4,
                  initializer=init_preload_carbon_data,
                  initargs=(workload, args.carbon_data_source, args.use_prediction,
                            args.desired_renewable_ratio)
                  ) as pool:
            result = pool.map(task_preload_carbon_data, unique_isos)
        for (iso, partial_carbon_data, ex, stack_trace) in result:
            if partial_carbon_data:
                carbon_data |= partial_carbon_data
            else:
                d_iso_errors[iso] = ex
                current_app.logger.error(f'Carbon data lookup failed for {iso}: {ex}')
                current_app.logger.error(stack_trace)

        with Pool(8,
                  initializer=init_parallel_process_candidate,
                  initargs=(workload, args.carbon_data_source, args.use_prediction, carbon_data)
                  ) as pool:
            result = pool.map(task_process_candidate, candidate_regions)
        for (region_name, iso, scores, d_misc, ex, stack_trace) in result:
            d_region_isos[region_name] = iso
            if not ex:
                d_region_scores[region_name] = scores
                d_misc_details[region_name] = d_misc
            else:
                if d_iso_errors.get(iso, None):
                    d_region_warnings[region_name] = d_iso_errors[iso]
                else:
                    d_region_warnings[region_name] = str(ex)
                    current_app.logger.error(f'Exception when calculating score for region {region_name}: {ex}')
                    current_app.logger.error(stack_trace)

        optimal_regions, d_weighted_scores = g_optimizer.compare_candidates(d_region_scores, True)
        if not optimal_regions:
            return orig_request | {
                'error': 'No viable candidate',
                'isos': d_region_isos,
                'details': d_region_warnings
            }, 400

        return orig_request | {
            'original-region': str(args.original_location),
            'optimal-regions': optimal_regions,
            'isos': d_region_isos,
            'weighted-scores': d_weighted_scores,
            'raw-scores': d_region_scores,
            'warnings': d_region_warnings,
            'details': d_misc_details,
        }
