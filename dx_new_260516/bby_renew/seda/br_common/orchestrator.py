import argparse
import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path


PYTHON = sys.executable


@dataclass(frozen=True)
class Step:
    number: int
    name: str
    module: str
    implemented: bool = False
    env: dict[str, str] = field(default_factory=dict)

    @property
    def key(self):
        return f"{self.number:02d}"


def default_steps(package_name, env_prefix):
    return [
        Step(1, "main_list", f"{package_name}.step01_main_list", implemented=True, env={f"{env_prefix}_MAIN_RUN_ID": "main"}),
        Step(2, "main_targets", f"{package_name}.step02_main_targets", env={f"{env_prefix}_MAIN_RUN_ID": "main"}),
        Step(3, "bsr_list", f"{package_name}.step03_bsr_list", env={f"{env_prefix}_BSR_RUN_ID": "bsr"}),
        Step(4, "bsr_rank", f"{package_name}.step04_bsr_rank", env={f"{env_prefix}_BSR_RUN_ID": "bsr"}),
        Step(5, "promotion_deals", f"{package_name}.step05_promotion_deals"),
        Step(6, "trending_deals", f"{package_name}.step06_trending_deals"),
        Step(7, "final_targets", f"{package_name}.step07_final_targets"),
        Step(8, "detail_enrichment", f"{package_name}.step08_detail_enrichment"),
        Step(9, "review20", f"{package_name}.step09_review20"),
        Step(10, "status_check", f"{package_name}.step10_status_check"),
        Step(11, "s3_sync", f"{package_name}.step11_s3_sync"),
        Step(12, "local_cleanup", f"{package_name}.step12_local_cleanup"),
        Step(13, "db_prepare", f"{package_name}.step13_db_prepare"),
        Step(14, "db_load", f"{package_name}.step14_db_load"),
    ]


def step_by_key(steps, value):
    for step in steps:
        if value in {step.key, step.name, str(step.number)}:
            return step
    raise SystemExit(f"Unknown step: {value}")


def selected_steps(steps, args):
    if args.all:
        return [step for step in steps if step.implemented]
    if args.from_step:
        start = step_by_key(steps, args.from_step).number
        return [step for step in steps if step.number >= start and step.implemented]
    if args.steps:
        return [step_by_key(steps, value) for value in args.steps]
    return []


def print_steps(display_name, steps):
    print(f"{display_name} pipeline steps:")
    for step in steps:
        status = "ready" if step.implemented else "planned"
        print(f"  {step.key} {step.name:<18} {status:<7} {step.module}")


def run_step(step, run_root, env_prefix, dry_run=False):
    if not step.implemented:
        print(f"[skip] step {step.key} {step.name}: not implemented yet")
        return 0
    env = os.environ.copy()
    env.setdefault(f"{env_prefix}_RUN_ROOT", str(run_root))
    env.update(step.env)
    command = [PYTHON, "-m", step.module]
    print(f"[run] step {step.key} {step.name}: {' '.join(command)}")
    print(f"      {env_prefix}_RUN_ROOT={env.get(f'{env_prefix}_RUN_ROOT')}")
    if dry_run:
        return 0
    return subprocess.call(command, env=env, cwd=Path(__file__).resolve().parent.parent)


def main(
    display_name,
    package_name,
    env_prefix,
    default_product_type,
    dated_run_root_func,
    product_types,
):
    parser = argparse.ArgumentParser(description=f"{display_name} crawler orchestrator")
    parser.add_argument("steps", nargs="*", help="Step numbers or names to run. Omit to list steps.")
    parser.add_argument("--from-step", dest="from_step", help="Run from this step through the last implemented step.")
    parser.add_argument("--all", action="store_true", help="Run all implemented steps.")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without running them.")
    parser.add_argument(
        "--product-type",
        "--category",
        dest="product_type",
        default=os.getenv(f"{env_prefix}_PRODUCT_TYPE", default_product_type),
        choices=product_types,
        help="Product type for the operational data folder.",
    )
    args = parser.parse_args()

    os.environ[f"{env_prefix}_PRODUCT_TYPE"] = str(args.product_type).strip().upper()
    os.environ.setdefault(
        f"{env_prefix}_RUN_ROOT",
        str(dated_run_root_func(product_type=os.environ[f"{env_prefix}_PRODUCT_TYPE"].lower())),
    )
    steps = default_steps(package_name, env_prefix)
    selected = selected_steps(steps, args)
    if not selected:
        print_steps(display_name, steps)
        return
    for step in selected:
        code = run_step(step, os.environ[f"{env_prefix}_RUN_ROOT"], env_prefix, dry_run=args.dry_run)
        if code:
            raise SystemExit(code)
