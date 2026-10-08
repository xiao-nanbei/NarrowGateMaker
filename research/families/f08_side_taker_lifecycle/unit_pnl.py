"""Unit-return reporting over already settled accounts, not an accounting engine."""
import math


def unit_returns(net_pnl, volume_btc, turnover_usdc):
    if not all(math.isfinite(x) for x in (net_pnl, volume_btc, turnover_usdc)):
        raise ValueError('unknown economics cannot become zero returns')
    if volume_btc < 0 or turnover_usdc < 0 or (volume_btc == 0) != (turnover_usdc == 0):
        raise ValueError('invalid two-sided volume or turnover')
    return dict(net_pnl_usdc=net_pnl, volume_btc=volume_btc, turnover_usdc=turnover_usdc,
        net_pnl_per_btc=net_pnl/volume_btc if volume_btc else None,
        net_pnl_per_10000_turnover=10000*net_pnl/turnover_usdc if turnover_usdc else None,
        net_pnl_per_0_001btc=.001*net_pnl/volume_btc if volume_btc else None)


def paired_comparison(baseline, candidate):
    """Exact common scope, ratio of sums, and descriptive scale residual.

    Each row must come from a settled account (or a separately validated daily
    equity slice). This does not infer a causal counterfactual, risk acceptance,
    confidence interval, or superiority to an information ablation.
    """
    def indexed(rows):
        result = {}
        for row in rows:
            key = (row['parent_account_id'], row['start_ns'], row['end_ns'], row['scenario_id'])
            if key in result or row.get('economic_complete') is not True:
                raise ValueError('duplicate or incomplete economic unit')
            unit_returns(row['net_pnl_usdc'], row['volume_btc'], row['turnover_usdc'])
            result[key] = row
        return result
    left, right = indexed(baseline), indexed(candidate)
    if not left or left.keys() != right.keys():
        raise ValueError('identical nonempty complete account scopes required')
    def summed(rows):
        return unit_returns(*(math.fsum(r[k] for r in rows.values()) for k in
            ('net_pnl_usdc', 'volume_btc', 'turnover_usdc')))
    b, c = summed(left), summed(right)
    differences = {k: None if b[k] is None or c[k] is None else c[k]-b[k]
                   for k in ('net_pnl_usdc', 'net_pnl_per_btc', 'net_pnl_per_10000_turnover')}
    retention = c['turnover_usdc']/b['turnover_usdc'] if b['turnover_usdc'] else None
    return dict(accounts=len(left), baseline=b, candidate=c, differences=differences,
        all_three_point_estimates_improved=all(x is not None and x > 0 for x in differences.values()),
        turnover_retention=retention,
        arithmetic_scale_residual=None if retention is None else c['net_pnl_usdc']-retention*b['net_pnl_usdc'],
        risk_acceptance='not_assessed', information_increment='not_assessed')
