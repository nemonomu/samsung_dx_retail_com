from br_common.orchestrator import main

from .step00_config import DEFAULT_PRODUCT_TYPE, ENV_PREFIX, PRODUCT_TYPES, r_magalu_dated_run_root


if __name__ == "__main__":
    main("R Magalu", "r_magalu", ENV_PREFIX, DEFAULT_PRODUCT_TYPE, r_magalu_dated_run_root, PRODUCT_TYPES)
