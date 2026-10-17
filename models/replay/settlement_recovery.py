"""Closed-journal postprocessing; deliberately no simulator imports."""
from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path

from execution.chunked_parquet_journal import iter_chunked_parquet_journal
from models.replay.public_accounting import settle_public_replay


def verify_manifest_relocation(original, relocated, *, original_sha256, relocated_sha256):
    """Accept only the exact named pair and the existing locator-only schema.

    Does not admit payloads: the existing ConsumerBundle validates their bytes.
    All ranges, ordered file identities, units and observation contracts remain
    byte-semantically identical after the explicitly enumerated substitutions.
    """
    a, b = Path(original).read_bytes(), Path(relocated).read_bytes()
    if sha256(a).hexdigest() != original_sha256 or sha256(b).hexdigest() != relocated_sha256:
        raise ValueError('relocation manifest identity mismatch')
    source, target = json.loads(a), json.loads(b)
    normalized = deepcopy(target)
    if normalized.pop('relocated_from_manifest_sha256', None) != original_sha256:
        raise ValueError('relocation parent mismatch')
    # A second host may rebind the already-relocated F03 input. Preserve its
    # first parent exactly; do not replace ancestry with an unrelated bundle.
    previous_parent = source.get('relocated_from_manifest_sha256')
    if previous_parent is not None:
        if ('relocation_previous_parent_sha256' in source or
                normalized.pop('relocation_previous_parent_sha256', None) != previous_parent):
            raise ValueError('relocation ancestry mismatch')
        normalized['relocated_from_manifest_sha256'] = previous_parent
    elif 'relocation_previous_parent_sha256' in normalized:
        raise ValueError('unexpected relocation ancestry')
    normalized['plan']['facts_root'] = source['plan']['facts_root']
    normalized['plan']['observation_profile']['measured_latency_path'] = source['plan']['observation_profile']['measured_latency_path']
    if len(normalized['source_bundles']) != len(source['source_bundles']):
        raise ValueError('relocation source count differs')
    for old, new in zip(source['source_bundles'], normalized['source_bundles'], strict=True):
        new['path'] = old['path']
    if normalized != source:
        raise ValueError('non-locator input difference')
    return dict(original_manifest_sha256=original_sha256, relocated_manifest_sha256=relocated_sha256,
                rule='facts_root_measured_latency_path_ordered_source_paths_only')


def settle_closed_journal(root, manifest, *, input_contract, funding, output,
                          initial_capital, max_mark_age_ns):
    """Idempotent settlement publication bound to immutable journal and inputs.

    No replay/checkpoint recovery and no incremental cash mutation. Repeated
    calls recompute the same pure accounting or reuse its exact bound receipt.
    """
    manifest, output = Path(manifest), Path(output)
    identity = dict(journal_sha256=sha256(manifest.read_bytes()).hexdigest(),
                    input_contract=input_contract, funding=funding,
                    initial_capital=initial_capital, max_mark_age_ns=max_mark_age_ns)
    binding = sha256(json.dumps(identity, sort_keys=True, allow_nan=False).encode()).hexdigest()
    fills, ends = [], []
    # Even on reuse, validate closed state and part integrity before accepting.
    for event in iter_chunked_parquet_journal(manifest):
        if event['event_type'] == 'fill':
            fills.append(event['record'])
        if event['event_type'] == 'account_end':
            ends.append(event['record'])
    if len(ends) != 1:
        raise ValueError('one closed account endpoint required')
    result = {**ends[0], '_fill_trace': fills, 'fills_total': len(fills),
              'public_input_contract': input_contract}
    account = settle_public_replay(root, result, initial_capital=initial_capital,
                                   max_mark_age_ns=max_mark_age_ns, funding=funding)
    receipt = dict(binding=binding, accounting=account,
                   recovery_kind='postprocessing_only_no_replay_no_checkpoint')
    if output.exists():
        if json.loads(output.read_text()) != receipt:
            raise ValueError('conflicting settlement publication')
        return receipt
    # One atomic publication; a failed write never becomes an accepted result.
    import os
    import tempfile
    fd, name = tempfile.mkstemp(prefix='.settlement-', dir=output.parent)
    try:
        with os.fdopen(fd, 'w') as handle:
            json.dump(receipt, handle, sort_keys=True, allow_nan=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(name, output)  # create-only even if another writer wins
    finally:
        os.unlink(name)
    return receipt
