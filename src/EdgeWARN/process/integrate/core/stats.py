import numpy as np

from ..config import output_decimals


def prepare_stats_specs(stats_config_list):
    stats_specs = [
        (
            conf["key"],
            conf["method"],
            conf["percentile"] if conf["method"] == "percentile" else None,
        )
        for conf in stats_config_list
    ]
    zero_results = {key: 0 for key, _, _ in stats_specs}
    unique_percentiles = sorted(
        {
            percentile
            for _, method, percentile in stats_specs
            if method == "percentile"
        }
    )
    needs_max = any(method == "max" for _, method, _ in stats_specs)
    needs_mean = any(method == "mean" for _, method, _ in stats_specs)
    return stats_specs, zero_results, unique_percentiles, needs_max, needs_mean


def sanitize_masked_values(masked_vals):
    masked_vals = masked_vals[~np.isnan(masked_vals)]
    masked_vals = masked_vals[masked_vals >= 0]
    return masked_vals


def reduce_stats(masked_vals, stats_specs, unique_percentiles, needs_max, needs_mean):
    percentile_cache = {}
    if unique_percentiles:
        percentile_values = np.percentile(masked_vals, unique_percentiles)
        percentile_cache = dict(zip(unique_percentiles, percentile_values))

    max_value = np.max(masked_vals) if needs_max else 0
    mean_value = np.mean(masked_vals) if needs_mean else 0

    decimals = output_decimals()
    result = {}
    for key, method, percentile in stats_specs:
        if method == "max":
            value = max_value
        elif method == "mean":
            value = mean_value
        elif method == "percentile":
            value = percentile_cache.get(percentile, 0)
        else:
            value = 0
        result[key] = round(float(value), decimals)

    return result
