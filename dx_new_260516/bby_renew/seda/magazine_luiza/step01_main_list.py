import os

from br_common.main_list_probe import run_main_list

from .step00_config import (
    DEFAULT_MAGAZINE_LUIZA_RUN_ROOT,
    ENV_PREFIX,
    load_magazine_luiza_initial_urls,
    magazine_luiza_product_type,
    magazine_luiza_run_date,
)


def main():
    product = magazine_luiza_product_type()
    run_main_list(
        retailer_key=ENV_PREFIX,
        product_type=product,
        run_date=magazine_luiza_run_date(),
        run_root=os.getenv(f"{ENV_PREFIX}_RUN_ROOT", str(DEFAULT_MAGAZINE_LUIZA_RUN_ROOT)),
        run_id=os.getenv(f"{ENV_PREFIX}_MAIN_RUN_ID", "main"),
        url_template=load_magazine_luiza_initial_urls(product_type=product).get("main", ""),
    )


if __name__ == "__main__":
    main()
