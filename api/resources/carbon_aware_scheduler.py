#!/usr/bin/env python3

from datetime import datetime
from flask_restful import Resource
from webargs import fields
from webargs.flaskparser import use_args, use_kwargs
import marshmallow_dataclass
from flask import current_app
from werkzeug.exceptions import NotImplemented

from api.resources.balancing_authority import convert_watttime_ba_abbrev_to_region, lookup_watttime_balancing_authority
from api.models.cloud_location import CloudLocationManager, CloudRegion
from api.models.workload import Workload
from api.models.optimization_engine import OptimizationEngine, OptimizationFactor

g_cloud_manager = CloudLocationManager()
OPTIMIZATION_FACTORS_AND_WEIGHTS = [
    (OptimizationFactor.CarbonIntensity, 1),
    (OptimizationFactor.WanNetworkUsage, 1),
]
g_optimizer = OptimizationEngine([t[0] for t in OPTIMIZATION_FACTORS_AND_WEIGHTS],
                                [t[1] for t in OPTIMIZATION_FACTORS_AND_WEIGHTS])

def get_alternative_regions(region: str) -> list[CloudRegion]:
    # NOTE: returns all possible regions for now, but can add filter/preference later.
    return g_cloud_manager.get_all_cloud_regions()

def assign_region_scores(region: str) -> dict[OptimizationFactor, float]:
    d_scores: dict[OptimizationFactor, float] = {}
    raise NotImplemented()
    return d_scores

carbon_aware_scheduler_args = {
    'start': fields.DateTime(format="iso", required=True),
    'end': fields.DateTime(format="iso", required=True),
}

class CarbonAwareScheduler(Resource):
    @use_kwargs(carbon_aware_scheduler_args, location='query')
    @use_args(marshmallow_dataclass.class_schema(Workload)())
    def get(self, args: Workload, start: datetime, end: datetime):
        workload = args
        orig_request = { 'request': {
            'start': start,
            'end': end,
            'workload': workload
        } }
        current_app.logger.info("CarbonAwareScheduler.get(%s, %s, %s)" % (start, end, workload))

        (latitude, longitude) = g_cloud_manager.get_gps_coordinate(args.preferred_cloud_location.cloud_provider,
                                                                args.preferred_cloud_location.region_code)
        watttime_lookup_result = lookup_watttime_balancing_authority(latitude, longitude)
        region = convert_watttime_ba_abbrev_to_region(watttime_lookup_result['watttime_abbrev'])

        candidate_regions = get_alternative_regions(region) + [region]
        l_region_scores = []
        l_region_names = []
        for region in candidate_regions:
            l_region_scores.append(assign_region_scores(region))
            l_region_names.append(region)
        index_best_region, l_weighted_score = g_optimizer.compare_candidates(l_region_scores, True)
        selected_region = l_region_names[index_best_region]

        return orig_request | watttime_lookup_result | {
            'requested-region': region,
            'selected-region': selected_region,
            'scores': { l_region_names[i]: l_weighted_score for i in range(candidate_regions) }
        }
