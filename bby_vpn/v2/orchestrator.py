"""CLI orchestration for ordered Best Buy V2 crawler steps."""

from __future__ import annotations

import argparse
import os
import sys
import time

from step_plan import PIPELINE_STEPS, STEP_BY_KEY, default_context


def parse_step_list(value: str | None) -> list[str] | None:
    if not value:
        return None
    result = [part.strip() for part in value.split(",") if part.strip()]
    unknown = [key for key in result if key not in STEP_BY_KEY]
    if unknown:
        raise SystemExit(f"Unknown step(s): {', '.join(unknown)}")
    return result


def selected_steps(only: str | None = None, start_at: str | None = None, stop_after: str | None = None):
    if only:
        wanted = set(parse_step_list(only) or [])
        return [step for step in PIPELINE_STEPS if step.key in wanted]

    keys = [step.key for step in PIPELINE_STEPS]
    start_index = keys.index(start_at) if start_at else 0
    stop_index = keys.index(stop_after) if stop_after else len(keys) - 1
    if stop_index < start_index:
        raise SystemExit("--stop-after must be the same as or later than --start-at")
    return list(PIPELINE_STEPS[start_index : stop_index + 1])


def run_pipeline(context, steps) -> int:
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    print(f"[PIPELINE] batch_id={context.batch_id} pages={context.pages}")
    for index, step in enumerate(steps, 1):
        started = time.time()
        print("\n" + "=" * 80)
        print(f"[STEP {index:02d}] {step.key}: {step.label}")
        print("=" * 80)
        status = step.runner(context)
        elapsed = time.time() - started
        print(f"[STEP {index:02d}] {step.key} completed status={status} elapsed={elapsed:.1f}s")
        if status:
            print(f"[PIPELINE] stopped at {step.key}")
            return status
    print("[PIPELINE] completed")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the ordered Best Buy V2 crawler pipeline.")
    parser.add_argument("--pages", type=int, default=None, help="Listing pages for main and BSR steps.")
    parser.add_argument("--batch-id", default=None)
    parser.add_argument("--only", help="Comma-separated step keys to run, preserving pipeline order.")
    parser.add_argument("--start-at", choices=[step.key for step in PIPELINE_STEPS])
    parser.add_argument("--stop-after", choices=[step.key for step in PIPELINE_STEPS])
    parser.add_argument("--skip-detail", action="store_true")
    parser.add_argument("--list-steps", action="store_true")
    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.list_steps:
        for step in PIPELINE_STEPS:
            print(f"{step.key}\t{step.label}")
        return 0
    context = default_context(pages=args.pages, batch_id=args.batch_id, skip_detail=args.skip_detail)
    steps = selected_steps(only=args.only, start_at=args.start_at, stop_after=args.stop_after)
    return run_pipeline(context, steps)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
