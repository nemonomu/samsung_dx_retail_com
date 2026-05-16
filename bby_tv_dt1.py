"""Compatibility entrypoint for the VPN Best Buy detail crawler.

Run this file from the repository root, or run bby_vpn/bby_tv_dt1.py directly.
Both commands now execute the same crawler implementation.
"""

import os
import sys


ROOT = os.path.dirname(os.path.abspath(__file__))
BBY_VPN_DIR = os.path.join(ROOT, "bby_vpn")
if BBY_VPN_DIR not in sys.path:
    sys.path.insert(0, BBY_VPN_DIR)

from bby_tv_dt1 import main  # noqa: E402


if __name__ == "__main__":
    main()
