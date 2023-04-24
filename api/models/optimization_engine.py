#!/usr/bin/env python3

from enum import Enum
from typing import Optional

from api.util import dict_min_key


class OptimizationFactor(str, Enum):
    EnergyUsage = 'energy-usage'
    CarbonEmission = 'carbon-emission'
    CarbonEmissionFromCompute = 'carbon-emission-from-compute'
    CarbonEmissionFromMigration = 'carbon-emission-from-migration'
    ElectricityPrice = 'electricity-price'
    DataCenterPUE = 'datacenter-pue'
    EnergyEfficiency = 'energy-efficiency'
    WanNetworkUsage = 'wan-network-usage'
    LanNetworkUsage = 'lan-network-usage'


class OptimizationEngine:
    """A simple optimization engine that takes weights of factors and compute the best candidates."""

    def __init__(self, factors: list[OptimizationFactor], weights: list[float]) -> None:
        if len(factors) != len(weights):
            raise ValueError("Length of factors and weight must be the same.")

        self.factors = factors
        self.weights = weights
        # self.factor_index_lookup_table = {}
        # for i in range(len(factors)):
        #     self.factor_index_lookup_table[factors[i]] = i

    def _calculate_weighted_score(self, score_dict: dict[OptimizationFactor, float]) -> float:
        total_weighted_score = 0.
        for i in range(len(self.factors)):
            factor = self.factors[i]
            weight = self.weights[i]
            score = score_dict[factor] if factor in score_dict else 0.
            total_weighted_score += score * weight
        return total_weighted_score / len(self.factors)

    def compare_candidates(self, d_scores: dict[str, dict[OptimizationFactor, float]], return_scores=False) -> \
            tuple[int, Optional[dict[str, float]]]:
        """Compares the candidate based on their scores in each factor and return the best candidate.

        Args:
            scores: a map from candidate name to a dict; the latter contains scores key'd by factor.
            return_scores: whether to return the calculated score.

        Returns:
            The index of the best candidate, and optionally the weighted score per candidate
        """
        if len(d_scores) == 0:
            return None, None if return_scores else None
        d_weighted_scores = dict()
        for candidate, scores in d_scores.items():
            weighted_score = self._calculate_weighted_score(scores)
            d_weighted_scores[candidate] = weighted_score
        return dict_min_key(d_weighted_scores, lambda p: p[1]), d_weighted_scores if return_scores else None
