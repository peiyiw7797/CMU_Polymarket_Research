"""Command-line interface for lookup queries."""

from __future__ import annotations

import argparse
import json
from typing import Iterable, Optional

from .search import candidate_summary, schedule_a_history, search_candidates, top_contributors


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="FEC candidate funding lookup CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    search_parser = subparsers.add_parser("search", help="Search candidates by name")
    search_parser.add_argument("query")
    search_parser.add_argument("--limit", type=int, default=None)

    summary_parser = subparsers.add_parser("summary", help="Show candidate summaries")
    summary_parser.add_argument("--cand-id", required=True)

    sched_parser = subparsers.add_parser("schedule-a", help="Retrieve Schedule A rows")
    sched_parser.add_argument("--cand-id", required=True)
    sched_parser.add_argument("--cycle", type=int, nargs="*")
    sched_parser.add_argument("--limit", type=int, default=200)
    sched_parser.add_argument("--page", type=int, default=1)

    top_parser = subparsers.add_parser("top-contributors", help="List top contributors")
    top_parser.add_argument("--cand-id", required=True)
    top_parser.add_argument("--limit", type=int, default=20)

    parser.add_argument("--json", action="store_true", help="Emit JSON instead of formatted text")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> None:
    args = parse_args(argv)
    if args.command == "search":
        results = search_candidates(args.query, limit=args.limit)
        payload = [match.__dict__ for match in results]
    elif args.command == "summary":
        payload = candidate_summary(args.cand_id)
    elif args.command == "schedule-a":
        payload = schedule_a_history(args.cand_id, cycles=args.cycle, limit=args.limit, page=args.page)
    elif args.command == "top-contributors":
        payload = top_contributors(args.cand_id, limit=args.limit)
    else:
        raise SystemExit(1)

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        if isinstance(payload, list):
            for row in payload:
                print(row)
        else:
            print(payload)


if __name__ == "__main__":
    main()
