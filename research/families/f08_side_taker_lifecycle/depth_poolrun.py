"""Finite research panel worker/validator for the existing PoolRun owner."""

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import sys

from research.families.f08_side_taker_lifecycle.depth_panel import run, validate_output


def worker(attempt_path):
    root = Path(attempt_path)
    attempt = json.loads((root / 'attempt.json').read_text())
    params = attempt['spec']['parameters']
    config = params['panel_config']
    if config['identity'] != params['cell']:
        raise ValueError('business panel identity differs')
    if shutil.disk_usage(root).free < 32*1024**3 + attempt['resources']['disk_bytes']:
        raise ValueError('32GiB execution reserve and declared panel budget required')
    binding = params['retained_journal']
    journal = Path(binding['path'])
    if hashlib.sha256(journal.read_bytes()).hexdigest() != binding['sha256']:
        raise ValueError('retained journal manifest changed')
    # No replay or training here. Continue from the already produced complete
    # journal and bound original ready observations under this one Attempt.
    temporary = root/'work'/'panel'
    result = run(config, temporary, journal=journal, production=binding['production'], compare=True)
    accepted = validate_output(temporary, params['cell'])
    output = root/'outputs'
    for name in accepted['files']:
        if (output/name).exists():
            raise ValueError('output already published')
        (temporary/name).rename(output/name)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--attempt')
    parser.add_argument('--validate', action='store_true')
    args = parser.parse_args()
    if args.validate:
        request = json.load(sys.stdin)
        print(json.dumps(validate_output(request['outputs'], request['attempt']['spec']['parameters']['cell'])))
    elif args.attempt:
        print(json.dumps(worker(args.attempt)))
    else:
        parser.error('choose --attempt or --validate')


if __name__ == '__main__':
    main()
