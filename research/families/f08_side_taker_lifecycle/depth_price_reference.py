"""Geometry-indexed causal reference for the new depth-quoting experiment.

The retired trailing-window estimator remains in depth_price_policy. These
states integrate time, not message counts, and never backfill a new radius.
"""

from dataclasses import dataclass
import math


def radius_grid(required):
    if type(required) is not int or required < 0:
        raise ValueError('nonnegative integer radius required')
    result = [0, 1]
    while result[-1] < required:
        k = result[-1]
        result.append(max(k + 1, (9 * k + 7) // 8))
    return result


@dataclass
class ReferenceMass:
    numerator: float = 0.0
    valid_mass: float = 0.0
    nominal_mass: float = 0.0

    def integrate(self, dt_ns, rho, half_life_ns):
        if dt_ns < 0:
            raise ValueError('reference clock regressed')
        if not dt_ns:
            return
        weight = -math.expm1(-math.log(2) * dt_ns / half_life_ns)
        decay = 1.0 - weight
        valid = rho is not None
        if valid and (not math.isfinite(rho) or rho < 0):
            raise ValueError('invalid observed density')
        self.numerator = decay * self.numerator + (weight * rho if valid else 0.0)
        self.valid_mass = decay * self.valid_mass + (weight if valid else 0.0)
        self.nominal_mass = decay * self.nominal_mass + weight

    def query(self):
        coverage = self.valid_mass / self.nominal_mass if self.nominal_mass else None
        # The tolerance only absorbs floating roundoff at the declared 60s
        # boundary; it is not a relaxed coverage or warmup threshold.
        ready = self.valid_mass >= .5 - 1e-14 and coverage is not None and coverage >= .95 - 1e-14
        return dict(available=ready,
                    reason=None if ready else ('reference_warmup' if self.valid_mass < .5 - 1e-14
                                               else 'reference_missing_mass'),
                    rho_ref=self.numerator / self.valid_mass if ready else None,
                    valid_mass=self.valid_mass, nominal_mass=self.nominal_mass,
                    weighted_coverage=coverage)


class GeometryEwma:
    """One shared bank; density is held only until its original source expiry."""

    def __init__(self, *, half_life_ns=60_000_000_000):
        if type(half_life_ns) is not int or half_life_ns <= 0:
            raise ValueError('positive reference half life required')
        self.half_life = half_life_ns
        self.clock = None
        self.expiry = None
        self.mass = {}
        self.current = {}

    def integrate_to(self, now):
        if type(now) is not int or now < 0 or (self.clock is not None and now < self.clock):
            raise ValueError('invalid reference clock')
        if self.clock is None:
            self.clock = now
        valid_end = min(now, max(self.clock, self.expiry or self.clock))
        for k, mass in self.mass.items():
            mass.integrate(valid_end - self.clock, self.current.get(k), self.half_life)
            mass.integrate(now - valid_end, None, self.half_life)
        self.clock = now

    def publish(self, *, now, expiry, densities):
        self.integrate_to(now)
        for k in densities:
            self.mass.setdefault(k, ReferenceMass())
        self.current = dict(densities)
        self.expiry = expiry

    def establish(self, k, rho):
        if self.clock is None:
            raise ValueError('reference has no source clock')
        self.mass.setdefault(k, ReferenceMass())
        self.current[k] = rho

    def query(self, k):
        return self.mass.get(k, ReferenceMass()).query()


def component_corrections(*, qb, qa, grid_points, reference, previous_qb=None,
                          previous_qa=None, endpoint_reason=None):
    """Missing T/D are neutral ACTION contributions, not fabricated values."""
    if type(grid_points) is not int or grid_points <= 0:
        raise ValueError('positive integer grid point count required')
    for x in (qb, qa, previous_qb, previous_qa, reference.get('rho_ref')):
        if x is not None and (not math.isfinite(x) or x < 0):
            raise ValueError('finite nonnegative quantity required')
    current_ok = qb is not None and qa is not None and qb + qa > 0
    q = qb + qa if current_ok else None
    rho = q / grid_points if current_ok else None
    r = reference.get('rho_ref') if reference.get('available') else None
    m_t = bool(current_ok and r is not None and rho + r > 0)
    m_d = bool(current_ok and previous_qb is not None and previous_qa is not None
               and q + previous_qb + previous_qa > 0)
    imbalance = (qb - qa) / q if current_ok else None
    thinness = (r - rho) / (r + rho) if m_t else None
    previous = previous_qb + previous_qa if m_d else None
    flow = ((qb - previous_qb) - (qa - previous_qa)) / (q + previous) if m_d else None
    depletion = (previous - q) / (q + previous) if m_d else None
    clip = lambda x: .1 * min(1.0, max(-1.0, x))
    static = dict(u=clip(imbalance) if current_ok else 0., v=clip(thinness or 0.) if current_ok else 0.)
    dynamic = dict(u=clip(imbalance + (flow or 0.)) if current_ok else 0.,
                   v=clip((thinness or 0.) + (depletion or 0.)) if current_ok else 0.)
    return dict(I=imbalance, T=thinness, F=flow, E=depletion, rho=rho,
                masks=dict(I=current_ok, T=m_t, D=m_d),
                reasons=dict(I=None if current_ok else 'current_quantity_unavailable_or_zero',
                             T=None if m_t else reference.get('reason') or 'current_region_invalid',
                             D=None if m_d else endpoint_reason or 'endpoint_unavailable'),
                static=static, dynamic=dynamic)
