"""Main entry point for the Best Buy V2 crawler pipeline."""

from __future__ import annotations

import sys

from orchestrator import main


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

