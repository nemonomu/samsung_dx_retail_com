from pathlib import Path

from br_common.config import absolute_url, dated_run_root, load_initial_urls, product_type, rel_path, run_date, url_for_page


R_MAGALU_BASE_URL = "https://www.magazineluiza.com.br"
PACKAGE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = PACKAGE_DIR / "config"
INITIAL_URLS_CSV = CONFIG_DIR / "r_magalu_initial_urls.csv"
ENV_PREFIX = "R_MAGALU"
DEFAULT_PRODUCT_TYPE = "HHP"
DEFAULT_RETAILER = "R_Magalu"
PRODUCT_TYPES = ["HHP"]

DEFAULT_URLS = {
    "main": f"{R_MAGALU_BASE_URL}/busca/smartphone/",
}


def r_magalu_run_date():
    return run_date(ENV_PREFIX)


def r_magalu_product_type():
    return product_type(ENV_PREFIX, DEFAULT_PRODUCT_TYPE)


def r_magalu_dated_run_root(run_date=None, product_type=None):
    return dated_run_root(PACKAGE_DIR, ENV_PREFIX, DEFAULT_PRODUCT_TYPE, run_date, product_type)


DEFAULT_R_MAGALU_RUN_ROOT = r_magalu_dated_run_root()


def load_r_magalu_initial_urls(path=INITIAL_URLS_CSV, product_type=None):
    return load_initial_urls(path, ENV_PREFIX, DEFAULT_URLS, product_type or r_magalu_product_type())


def target_url(page_type, product_type=None):
    return load_r_magalu_initial_urls(product_type=product_type).get(page_type, "")


def absolute_r_magalu_url(path):
    return absolute_url(R_MAGALU_BASE_URL, path)
