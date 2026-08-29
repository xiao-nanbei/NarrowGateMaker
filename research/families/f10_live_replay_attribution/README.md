# F10 Live/Replay Attribution

F10 contains diagnostic methods for replay/live implementation parity, clock
and unit contracts, lifecycle attribution, campaign accounting, and loss
localization. These diagnostics can identify engineering and estimand defects;
they do not independently establish alpha or authorize a live action.

## Public boundary

The public repository contains reusable code, method descriptions, and
explicitly public non-operational fixtures. Current hosts, regions, instance
types, account state, order/fill identifiers, active policy identity, release
and rollback records, session tokens, runtime profiles, and exact operational
receipts are maintained in the private operations repository.

Public replay uses an explicit config or the checked-in public template. It
does not resolve a current-live pointer and must not infer current deployment
state from historical research artifacts. A missing owner input fails closed;
it is never replaced with a public placeholder carrying real operational
values.

## Research interpretation

Touch probability, fill probability, queue position, adverse selection,
campaign value, and inventory continuation are separate estimands. Any result
called PnL or EV must state its currency unit and denominator. Python/C++
parity proves implementation agreement only; economic validity requires an
independent estimand and evidence gate.

Action-changing work uses frozen chronological panels and paired full-path
replay. Modeled queue results are diagnostic and cannot authorize live action.
Formal replay requires a dense causal one-second clock and the declared native
book/queue inputs; missing inputs fail closed.

See the repository-level [research guide](../../README.md) and
[public/private contract](../../../docs/public_private_documentation_contract.md).
