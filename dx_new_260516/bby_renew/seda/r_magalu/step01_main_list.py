import os

from br_common.main_list_probe import run_main_list

from .step00_config import (
    DEFAULT_R_MAGALU_RUN_ROOT,
    ENV_PREFIX,
    load_r_magalu_initial_urls,
    r_magalu_product_type,
    r_magalu_run_date,
)


def main():
    product = r_magalu_product_type()
    run_main_list(
        retailer_key=ENV_PREFIX,
        product_type=product,
        run_date=r_magalu_run_date(),
        run_root=os.getenv(f"{ENV_PREFIX}_RUN_ROOT", str(DEFAULT_R_MAGALU_RUN_ROOT)),
        run_id=os.getenv(f"{ENV_PREFIX}_MAIN_RUN_ID", "main"),
        url_template=load_r_magalu_initial_urls(product_type=product).get("main", ""),
    )


if __name__ == "__main__":
    main()
