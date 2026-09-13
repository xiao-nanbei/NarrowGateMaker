# BER Role-Safe Add-Only v1 Mechanics Failure

Last materially modified: 2026-08-08

The frozen v1 execution contract is closed before the 40-day Development read. On `2026-04-17`, Python and C++ produced identical BER state and fill paths within both arms. Source-selection and spread-cap invariants also had zero violations, and the candidate changed both BUY and SELL prices.

The failed condition was an invalid cross-arm requirement. Control emitted `14,593` requotes and candidate emitted `14,536`; the runner then compared BER EMA state at those different terminal times. A full-path price action can legitimately change fills, inventory, circuit-breaker continuation, and the number of later quote decisions. Equal arm-level terminal clocks are therefore not a valid mechanics invariant.

The successor keeps the exact same policy, signal, `1.2` threshold, `2.0` multiplier, panel, and risk gates. It removes only cross-arm terminal-state equality, measures action support on candidate-arm canonical side decisions, and reports control/candidate requote counts as lifecycle outcomes. Within-arm Python/C++ BER-state and fill-path lockstep remains zero tolerance.

The one-day economic output is consumed and cannot be used to tune or select the successor. No Validation or holdout was read, and no action or live authority was created.
