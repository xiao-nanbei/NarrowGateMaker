"""Explicit local replay plans; paths are not economic account identities.

This first plan profile admits the existing JSON-configured ConsumerBundle
executor. It does not load live defaults, import arbitrary policy objects, or
claim support for historical private overlays.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import math
from pathlib import Path

SCHEMA = "narrowgate.replay-plan.v1"
RESOLVED_SCHEMA = "narrowgate.resolved-replay-plan.v1"


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, allow_nan=False, separators=(",", ":")).encode()
    ).hexdigest()


def file_digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def source_identity():
    """Bind the installed transitional runtime, not Git HEAD or private data.

    Research dependencies remain included until their actual consumers migrate.
    This is a cold source-only traversal, never an input/cache hash traversal.
    """
    files = {}
    for name in (
        "narrowgate",
        "data",
        "features",
        "execution",
        "models",
        "strategy",
        "live",
        "research",
    ):
        spec = importlib.util.find_spec(name)
        if spec is None or not spec.submodule_search_locations:
            raise ValueError(f"runtime package unavailable: {name}")
        root = Path(next(iter(spec.submodule_search_locations)))

        def visit(directory):
            for path in sorted(directory.iterdir()):
                if path.is_symlink():
                    raise ValueError(f"runtime source symlink is not admitted: {path}")
                if path.is_dir() and path.name != "private" and (path / "__init__.py").is_file():
                    yield from visit(path)
                elif path.is_file() and path.suffix == ".py":
                    yield path

        for path in visit(root):
            files[f"{name}/{path.relative_to(root)}"] = file_digest(path)
    for name in (
        "calendar_features",
        "data_paths",
        "data_quality",
        "market_fusion",
        "narrowgate_cpp",
    ):
        spec = importlib.util.find_spec(name)
        if spec is not None and spec.origin and Path(spec.origin).is_file():
            files[name] = file_digest(spec.origin)
    return digest(files)


def _object(value, keys, field):
    if not isinstance(value, dict) or set(value) != set(keys):
        raise ValueError(f"{field}: expected fields {sorted(keys)}")
    return value


def resolve(path):
    """Validate one plan without preparing data or executing economics."""
    from data.runtime import ConsumerBundle

    path = Path(path).resolve(strict=True)
    plan = json.loads(path.read_text())
    _object(
        plan,
        {
            "schema",
            "run_id",
            "identity",
            "bundle",
            "params",
            "model_dir",
            "funding",
            "accounting",
            "output",
        },
        "plan",
    )
    if plan["schema"] != SCHEMA:
        raise ValueError("plan.schema: unsupported replay plan")
    run_id = plan["run_id"]
    if (
        not isinstance(run_id, str)
        or not run_id
        or any(
            c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
            for c in run_id
        )
    ):
        raise ValueError("plan.run_id: use letters, digits, '-' or '_'")

    def locator(value, field):
        if not isinstance(value, str) or not value:
            raise ValueError(f"plan.{field}: explicit path required")
        candidate = Path(value).expanduser()
        return (
            (path.parent / candidate).resolve()
            if not candidate.is_absolute()
            else candidate.resolve()
        )

    bundle_path = locator(plan["bundle"], "bundle")
    bundle = ConsumerBundle(bundle_path)
    identity = _object(
        plan["identity"],
        {"experiment_id", "baseline_id", "arm_id", "account_contract", "parent_id"},
        "plan.identity",
    )
    if any(not isinstance(v, str) or not v for v in identity.values()):
        raise ValueError("plan.identity: nonempty strings required")
    params = plan["params"]
    if not isinstance(params, dict):
        raise ValueError("plan.params: explicit effective parameters required")
    parameter_sources = {}
    if "base_file" in params:
        _object(params, {"base_file", "overrides"}, "plan.params")
        base_path = locator(params["base_file"], "params.base_file")
        base = json.loads(base_path.read_text())
        overrides = params["overrides"]
        if not isinstance(base, dict) or not isinstance(overrides, dict):
            raise ValueError("plan.params: base and overrides must be JSON objects")
        if set(overrides) - set(base):
            raise ValueError(
                f"plan.params.overrides: unknown base keys {sorted(set(overrides) - set(base))}"
            )
        parameter_sources = {
            key: (f"{path}#/params/overrides/{key}" if key in overrides else f"{base_path}#/{key}")
            for key in base
        }
        params = {**base, **overrides}
    else:
        parameter_sources = {key: f"{path}#/params/{key}" for key in params}
    required = {"tick_size", "account_start_ns", "rng_seed", "latency_seed", "ml_enabled"}
    if not required.issubset(params):
        raise ValueError(f"plan.params: missing {sorted(required - params.keys())}")
    if any(k in params for k in ("_l2_journal", "_replay_progress_callback", "_target_journal")):
        raise ValueError("plan.params: operational callbacks/sinks belong to the runner")
    if (
        params.get("initial_live_state")
        or params.get("replay_initial_state_mode", "fresh_start") != "fresh_start"
    ):
        raise ValueError("plan.params: new accounts start once; recovery uses replay resume")
    start, end = params["account_start_ns"], bundle.manifest["plan"]["end_ns"]
    if (
        type(start) is not int
        or start % 10**6
        or end % 10**6
        or not bundle.manifest["plan"]["start_ns"] <= start < end
    ):
        raise ValueError("plan.params.account_start_ns: exact-ms account inside bundle required")
    if any(type(params[k]) is not int for k in ("rng_seed", "latency_seed")):
        raise ValueError("plan.params: explicit integer RNG seeds required")
    if type(params["ml_enabled"]) is not bool:
        raise ValueError("plan.params.ml_enabled: explicit boolean required")
    accounting = _object(
        plan["accounting"], {"initial_capital", "max_mark_age_ns"}, "plan.accounting"
    )
    capital, age = accounting["initial_capital"], accounting["max_mark_age_ns"]
    if (
        type(capital) not in (int, float)
        or not math.isfinite(capital)
        or capital <= 0
        or type(age) is not int
        or age < 0
    ):
        raise ValueError(
            "plan.accounting: positive finite capital and nonnegative integer mark age required"
        )
    funding_path = locator(plan["funding"], "funding")
    funding = json.loads(funding_path.read_text())
    from models.replay.public_accounting import funding_window

    funding = funding_window(funding, start_ns=start, end_ns=end)
    if funding["market_id"] != bundle.manifest["plan"]["market_id"]:
        raise ValueError("plan.funding: market differs from input bundle")
    from models.replay.continuous_accounting import funding_cashflow_usdc

    for event in funding["events"]:
        funding_cashflow_usdc(0.0, event["mark_price"], event["rate"])
    model_dir = None
    model_files = None
    if plan["model_dir"] is not None:
        model_dir = locator(plan["model_dir"], "model_dir")
        if not model_dir.is_dir():
            raise ValueError(f"plan.model_dir: directory missing: {model_dir}")
        model_files = {}
        for item in sorted(model_dir.rglob("*")):
            if item.is_symlink():
                raise ValueError(f"plan.model_dir: symlink not admitted: {item}")
            if item.is_file():
                model_files[str(item.relative_to(model_dir))] = file_digest(item)
        if not model_files:
            raise ValueError("plan.model_dir: empty model directory")
    if params["ml_enabled"] != (model_dir is not None):
        raise ValueError("plan.model_dir: must be present exactly when ml_enabled is true")
    output = locator(plan["output"], "output")
    for protected in (bundle_path, funding_path.parent, model_dir):
        if protected is not None and (output == protected or protected.is_relative_to(output)):
            raise ValueError("plan.output must not contain an input/model directory")
    if any(output.is_relative_to(p) for p in (bundle_path, model_dir) if p is not None):
        raise ValueError("plan.output must not be inside an input/model directory")
    binding = dict(
        identity=identity,
        params=params,
        accounting=accounting,
        account_end_ns=end,
        funding=funding,
        input_manifest_id=bundle.input_manifest_id,
        model_files=model_files,
        release_identity=source_identity(),
    )
    resolved = dict(
        schema=RESOLVED_SCHEMA,
        run_id=run_id,
        economic_identity=digest(binding),
        binding=binding,
        locations=dict(
            bundle=str(bundle_path),
            funding=str(funding_path),
            model_dir=None if model_dir is None else str(model_dir),
            output=str(output),
        ),
        source_plan=str(path),
        provenance={key: f"{path}#/{key}" for key in plan},
        parameter_sources=parameter_sources,
        units=dict(
            account_start_ns="UTC nanoseconds",
            initial_capital="USDC",
            max_mark_age_ns="nanoseconds",
            tick_size="USDC/BTC",
        ),
        profile="consumer_bundle_json_single_account",
        limitations=[
            "transitional Python executor; not yet decomposed",
            "no private runtime-object policy assembly",
            "local executor only; not PoolRun admission",
        ],
    )
    return resolved


def account_identity(resolved):
    b = resolved["binding"]
    return dict(
        b["identity"],
        start_ns=b["params"]["account_start_ns"],
        end_ns=b["account_end_ns"],
        input_manifest_id=b["input_manifest_id"],
        release_identity=b["release_identity"],
        seed_pair={k: b["params"][k] for k in ("rng_seed", "latency_seed")},
        run_id=resolved["run_id"],
        economic_identity=resolved["economic_identity"],
    )
