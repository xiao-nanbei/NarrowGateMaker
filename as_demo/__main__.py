"""Run the fictional demo without network access or file outputs."""
import argparse
import json
from dataclasses import replace

from .replay import replay
from .synthetic import events


def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument('--seed', type=int, default=7)
    parser.add_argument('--events', type=int, default=200)
    args = parser.parse_args()
    if args.events < 2:
        parser.error('--events must be >= 2')
    # Invented teaching inputs; no calibration or production configuration.
    stream = (replace(e, time=e.time / (args.events-1))
              for e in events(args.events, args.seed))
    result = replay(stream, horizon=1, risk_aversion=0.2, volatility=0.8, liquidity=2)
    print(json.dumps({'kind': 'fictional_teaching_run_not_research_evidence',
                      'events': args.events, 'fills': len(result.fills),
                      'cash': str(result.cash), 'inventory': str(result.inventory),
                      'fees': str(result.fees), 'marked_equity': str(result.equity)}, indent=2))


if __name__ == '__main__':
    main()
