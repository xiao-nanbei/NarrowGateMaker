# F03 1s Individual-Trade Reference Repair v1

Date: 2026-08-05

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Status: reference repair admitted; no prediction, economic, action, baseline, or live authority

## Scope

This implementation repairs only the seven missing BTCUSDT official individual-trade 1s reference artifacts:

`2026-04-12`, `2026-05-07`, `2026-05-08`, `2026-05-10`, `2026-05-11`, `2026-05-14`, and `2026-05-16`.

It does not modify frozen panels, `causal_v12_1s_orico_source_spec.py`, metrics inputs, predictions, or PnL. The existing same-day files under `bars_1s` were not used because those artifacts are aggTrades-derived and are not the required individual-trade authority.

## Admission

The repair runner is [causal_v12_1s_individual_reference_repair.py](../audit/causal_v12_1s_individual_reference_repair.py). It requires the exact raw path and header, validates strictly increasing trade IDs, nondecreasing exchange timestamps, UTC-day containment, side accounting, the `[t,t+1s)` bar clock, row counts, and raw/output SHA256 identities.

It writes staged parquet and sidecar files in the formal `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` reference root. The parquet is published first; the `.parquet.meta.json` sidecar is published last and is the admission marker. An existing artifact can only be reused or replaced after the full pair passes validation.

The seven days bind 19,931,644 raw trades and 570,739 observed 1s bars. Total parquet output is 22,530,739 bytes.

Formal root:

```text
${NARROWGATE_DATA_ROOT}/reference_bars_1s_trades_v1
```

Authoritative batch manifest:

```text
${NARROWGATE_DATA_ROOT}/reference_bars_1s_trades_v1/admission_manifests/causal_v12_1s_2026_native_reference_repair_v1_20260805.json
SHA256 a9042fb74969cd7c177d74bf2d26797252177992c64e521db1698c53a584201d
```

The machine-readable implementation record is [causal_v12_1s_individual_reference_repair_v1_implementation_20260805.json](causal_v12_1s_individual_reference_repair_v1_implementation_20260805.json).

## Mendel Rerun

The exact-profile auditor can be rerun without changing any denominator:

```bash
.venv/bin/python -m research.families.f03_causal_13_head.audit.causal_v12_1s_2026_native_source_coverage \
  --market-data-root ${NARROWGATE_DATA_ROOT} \
  --output ${NARROWGATE_EPHEMERAL_ROOT}/causal_v12_1s_2026_native_source_coverage_post_reference_repair_final_20260805.json
```

Required reference manifest is the `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` batch manifest above. The observed postcheck produced SHA256 `70cec635c523471f26ee4822b78feec425da359ed60e9a0218f8423c29bf563e` and reported:

| Panel | Exact support |
|---|---:|
| historical native transport Development | 22/22 |
| historical native late diagnostic | 22/22 |
| combined 22+22 | 44/44 |
| frozen Development | 40/40 |

This 44/44 result is a joint exact-profile postcheck. The claim made here is only that the seven-day individual-reference blocker is cleared. The separate metrics fix is neither implemented nor attributed by this report.

## Permission Boundary

The repair admits source artifacts only. It does not authorize feature materialization, training, transport scoring, economic replay, an action, baseline replacement, or live deployment.
