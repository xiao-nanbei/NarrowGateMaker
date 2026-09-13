"""Persistent research-only native development build; never installs a wheel."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import sysconfig
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jobs", type=int, default=2)
    parser.add_argument("--profile", choices=("portable", "host-native"), default="portable")
    parser.add_argument("--release", action="store_true")
    parser.add_argument("--build-root", type=Path, default=Path(".build/replay"))
    args = parser.parse_args()
    if args.jobs < 1:
        parser.error("jobs must be positive")
    root = Path(__file__).resolve().parents[1]
    for tool in ("cmake", "ninja"):
        if not shutil.which(tool):
            parser.error(f"missing {tool}")
    compiler = shutil.which(os.environ.get("CXX", "c++"))
    if compiler is None:
        parser.error("missing C++ compiler")
    version = subprocess.check_output([compiler, "--version"], text=True)
    identity = hashlib.sha256((compiler + version).encode()).hexdigest()[:12]
    abi = sysconfig.get_config_var("SOABI")
    kind = "Release" if args.release else "RelWithDebInfo"
    build = (
        args.build_root / f"{abi}-{platform.machine()}-{identity}-{args.profile}-{kind}"
    ).resolve()
    pybind = subprocess.check_output(
        [sys.executable, "-m", "pybind11", "--cmakedir"], text=True
    ).strip()
    configure = [
        "cmake",
        "-S",
        str(root / "cpp"),
        "-B",
        str(build),
        "-G",
        "Ninja",
        f"-DPython_EXECUTABLE={sys.executable}",
        f"-Dpybind11_DIR={pybind}",
        f"-DCMAKE_CXX_COMPILER={compiler}",
        f"-DCMAKE_BUILD_TYPE={kind}",
        "-DNARROWGATE_BUILD_FLAVOR=full",
        f"-DNARROWGATE_LIVE_CPU_PROFILE={args.profile}",
        "-DCMAKE_INTERPROCEDURAL_OPTIMIZATION=OFF",
    ]
    cache = shutil.which("ccache")
    configure.append(f"-DCMAKE_CXX_COMPILER_LAUNCHER={cache or ''}")
    start = time.perf_counter()
    subprocess.run(configure, check=True)
    configured = time.perf_counter()
    subprocess.run(
        [
            "cmake",
            "--build",
            str(build),
            "--target",
            "narrowgate_cpp",
            "--parallel",
            str(args.jobs),
        ],
        check=True,
    )
    built = time.perf_counter()
    environment = dict(os.environ, PYTHONPATH=str(build) + os.pathsep + str(root))
    loaded = subprocess.check_output(
        [sys.executable, str(root / "scripts/run_replay_native.py"), "--build-dir", str(build)],
        env=environment,
        text=True,
    ).strip()
    if Path(loaded).resolve().parent != build:
        raise RuntimeError("new worker loaded a different extension")
    record = dict(
        build_directory=str(build),
        native_module=loaded,
        compiler=version.splitlines()[0],
        profile=args.profile,
        build_type=kind,
        ccache=bool(cache),
        jobs=args.jobs,
        configure_seconds=configured - start,
        build_seconds=built - configured,
        source_commit=subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root, text=True
        ).strip(),
        dirty=bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=root)),
    )
    with (build / "build-measurements.jsonl").open("a") as handle:
        handle.write(json.dumps(record) + "\n")
    print(json.dumps(record, indent=2))


if __name__ == "__main__":
    main()
