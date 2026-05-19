import argparse
import json
from pathlib import Path

from .main_list_probe import parse_products


def main():
    parser = argparse.ArgumentParser(description="Analyze a captured retail HTML/JSON response for product rows.")
    parser.add_argument("path", help="Captured HTML or JSON response file.")
    parser.add_argument("--page", type=int, default=1, help="Page number to assign to parsed rows.")
    parser.add_argument("--out", help="Optional JSON output path for parsed product candidates.")
    args = parser.parse_args()

    path = Path(args.path)
    text = path.read_text(encoding="utf-8", errors="replace")
    rows = parse_products(text, args.page)
    payload = {
        "path": str(path),
        "bytes": path.stat().st_size,
        "row_count": len(rows),
        "sample_rows": rows[:5],
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    if args.out:
        Path(args.out).write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")


if __name__ == "__main__":
    main()
