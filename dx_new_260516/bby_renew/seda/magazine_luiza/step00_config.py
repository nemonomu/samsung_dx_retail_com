from pathlib import Path

from br_common.config import absolute_url, dated_run_root, load_initial_urls, product_type, rel_path, run_date, url_for_page


MAGAZINE_LUIZA_BASE_URL = "https://www.magazineluiza.com.br"
PACKAGE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = PACKAGE_DIR / "config"
INITIAL_URLS_CSV = CONFIG_DIR / "magazine_luiza_initial_urls.csv"
ENV_PREFIX = "MAGAZINE_LUIZA"
DEFAULT_PRODUCT_TYPE = "TV"
DEFAULT_RETAILER = "Magazine_Luiza"
PRODUCT_TYPES = ["TV", "LDY"]

DEFAULT_URLS = {
    "main": f"{MAGAZINE_LUIZA_BASE_URL}/busca/tv/",
}


def magazine_luiza_run_date():
    return run_date(ENV_PREFIX)


def magazine_luiza_product_type():
    return product_type(ENV_PREFIX, DEFAULT_PRODUCT_TYPE)


def magazine_luiza_dated_run_root(run_date=None, product_type=None):
    return dated_run_root(PACKAGE_DIR, ENV_PREFIX, DEFAULT_PRODUCT_TYPE, run_date, product_type)


DEFAULT_MAGAZINE_LUIZA_RUN_ROOT = magazine_luiza_dated_run_root()


def load_magazine_luiza_initial_urls(path=INITIAL_URLS_CSV, product_type=None):
    return load_initial_urls(path, ENV_PREFIX, DEFAULT_URLS, product_type or magazine_luiza_product_type())


def target_url(page_type, product_type=None):
    return load_magazine_luiza_initial_urls(product_type=product_type).get(page_type, "")


def absolute_magazine_luiza_url(path):
    return absolute_url(MAGAZINE_LUIZA_BASE_URL, path)
