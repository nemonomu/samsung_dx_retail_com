from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from siel.csv_export import export


PYTHON = sys.executable
PACKAGE_DIR = Path(__file__).resolve().parent
LOGS_DIR = PACKAGE_DIR / "logs"


@dataclass(frozen=True)
class Step:
    number: int
    name: str
    description: str

    @property
    def key(self) -> str:
        return f"{self.number:02d}"


STEPS = [
    Step(1, "main_list", "Amazon India search listing"),
    Step(3, "bsr_list", "Amazon India best-seller listing"),
    Step(8, "detail_enrichment", "Amazon India PDP detail enrichment"),
    Step(14, "csv_export", "Export JSONL run output to final/product-list CSV"),
]


def latest_jsonl(product: str) -> Path | None:
    pattern = f"siel_amazon_{product.lower()}_run_*.jsonl"
    matches = sorted(LOGS_DIR.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return matches[0] if matches else None


def print_steps():
    print("SIEL Amazon India pipeline steps:")
    for step in STEPS:
        print(f"  {step.key} {step.name:<18} {step.description}")


def main() -> int:
    parser = argparse.ArgumentParser(description="SIEL Amazon India crawler orchestrator")
    parser.add_argument("--product-type", "--product", "--category", dest="product", default="tv",
                        choices=["hhp", "tv", "ref", "ldy"])
    parser.add_argument("--stages", nargs="+", default=["main", "bsr", "detail"],
                        choices=["main", "bsr", "detail"])
    parser.add_argument("--max-rank", type=int, default=300)
    parser.add_argument("--bsr-max-rank", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=30)
    parser.add_argument("--max-detail", type=int, default=None)
    parser.add_argument("--detail-sleep", type=float, default=2.0)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--export-latest", action="store_true",
                        help="Skip crawl and export the latest matching JSONL run.")
    parser.add_argument("--jsonl-path", default="", help="Explicit JSONL path for csv_export.")
    parser.add_argument("--output-dir", default="", help="CSV output directory.")
    args = parser.parse_args()

    if not args.export_latest and not args.jsonl_path:
        command = [
            PYTHON, "-m", "siel.amazon.run",
            "--product", args.product,
            "--stages", *args.stages,
            "--max-rank", str(args.max_rank),
            "--bsr-max-rank", str(args.bsr_max_rank),
            "--max-pages", str(args.max_pages),
            "--detail-sleep", str(args.detail_sleep),
            "--no-auto-insert",
        ]
        if args.max_detail is not None:
            command.extend(["--max-detail", str(args.max_detail)])
        if args.headless:
            command.append("--headless")
        print(f"[run] {' '.join(command)}")
        if args.dry_run:
            print_steps()
            return 0
        code = subprocess.call(command, cwd=PACKAGE_DIR.parent.parent)
        if code:
            return code

    jsonl = Path(args.jsonl_path) if args.jsonl_path else latest_jsonl(args.product)
    if not jsonl:
        print(f"[skip] no JSONL found for product={args.product}", file=sys.stderr)
        return 2
    output_dir = Path(args.output_dir) if args.output_dir else jsonl.parent / (jsonl.stem + "_output")
    manifest = export(jsonl, output_dir)
    print(json.dumps(manifest, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
