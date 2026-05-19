from br_common.orchestrator import main

from .step00_config import DEFAULT_PRODUCT_TYPE, ENV_PREFIX, PRODUCT_TYPES, magazine_luiza_dated_run_root


if __name__ == "__main__":
    main(
        "Magazine Luiza",
        "magazine_luiza",
        ENV_PREFIX,
        DEFAULT_PRODUCT_TYPE,
        magazine_luiza_dated_run_root,
        PRODUCT_TYPES,
    )
