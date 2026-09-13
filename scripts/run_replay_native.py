"""Load the explicit research artifact before any package editable import hook."""

import argparse
import importlib.util
from pathlib import Path
import runpy
import sys
import sysconfig


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", required=True, type=Path)
    parser.add_argument("--module")
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    path = args.build_dir.resolve() / ("narrowgate_cpp" + sysconfig.get_config_var("EXT_SUFFIX"))
    spec = importlib.util.spec_from_file_location("narrowgate_cpp", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    sys.modules["narrowgate_cpp"] = module
    if args.module:
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        sys.argv = [args.module, *args.arguments]
        runpy.run_module(args.module, run_name="__main__")
    else:
        print(module.__file__)


if __name__ == "__main__":
    main()
