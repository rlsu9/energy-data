#!/usr/bin/env python3

from enum import Enum
from typing import Optional

import numpy as np
from werkzeug.exceptions import BadRequest


class OptimizationFactor(str, Enum):
    EnergyUsage = 'energy-usage'
    CarbonEmission = 'carbon-emission'
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

    def compare_candidates(self, scores: list[dict[OptimizationFactor, float]], return_scores=False) -> \
            tuple[int, Optional[list[float]]]:
        """Compares the candidate based on their scores in each factor and return the best candidate.

        Args:
            scores: a list of dicts that contains scores key'd by factor.
            return_scores: whether to return the calculated score.

        Returns:
            The index of the best candidate, and optionally the weighted score per candidate
        """
        if len(scores) == 0:
            raise BadRequest('No viable candidate.')
        l_weighted_scores = []
        for candidate_index in range(len(scores)):
            weighted_score = self._calculate_weighted_score(scores[candidate_index])
            l_weighted_scores.append(weighted_score)
        return np.argmin(l_weighted_scores), l_weighted_scores if return_scores else None
