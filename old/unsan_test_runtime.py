from __future__ import annotations

import importlib.util
import os
import re
import sys
import types
from pathlib import Path


OLD_ROOT = Path(__file__).resolve().parent
REPO_ROOT = OLD_ROOT.parent
COMMON_OLD = OLD_ROOT / "tv, hhp_bestbuy_common_old"


def install_old_common_package() -> None:
    """Expose the archived Unsan common modules as ``common.*``."""
    for path in (OLD_ROOT, REPO_ROOT):
        text = str(path)
        if text not in sys.path:
            sys.path.insert(0, text)

    if "common" not in sys.modules:
        package = types.ModuleType("common")
        package.__path__ = [str(COMMON_OLD)]
        sys.modules["common"] = package

    for name in ("setup", "data_extractor", "alert_hhp_monitor", "base_crawler"):
        module_name = f"common.{name}"
        if module_name in sys.modules:
            continue
        module_path = COMMON_OLD / f"{name}.py"
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        if not spec or not spec.loader:
            raise ImportError(f"Cannot load {module_path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        setattr(sys.modules["common"], name, module)
        spec.loader.exec_module(module)


def install_sql_table_rewrite(table_map: dict[str, str]) -> None:
    """Route archived crawler SQL to the requested test tables."""
    import psycopg2
    from psycopg2.extensions import cursor as PsycopgCursor

    if getattr(psycopg2.connect, "_unsan_test_rewrite", False):
        return

    original_connect = psycopg2.connect
    placeholders = {source: f"__UNSAN_TABLE_{index}__" for index, source in enumerate(table_map)}

    def rewrite_sql(sql):
        if not isinstance(sql, str):
            return sql
        rewritten = sql
        for source, placeholder in placeholders.items():
            rewritten = re.sub(rf"\b{re.escape(source)}\b", placeholder, rewritten)
        for source, placeholder in placeholders.items():
            rewritten = rewritten.replace(placeholder, table_map[source])
        return rewritten

    class RewritingCursor(PsycopgCursor):
        def execute(self, query, vars=None):
            return super().execute(rewrite_sql(query), vars)

        def executemany(self, query, vars_list):
            return super().executemany(rewrite_sql(query), vars_list)

    def connect_with_rewrite(*args, **kwargs):
        kwargs.setdefault("cursor_factory", RewritingCursor)
        return original_connect(*args, **kwargs)

    connect_with_rewrite._unsan_test_rewrite = True
    psycopg2.connect = connect_with_rewrite


def install_test_runtime(table_map: dict[str, str]) -> None:
    install_old_common_package()
    install_sql_table_rewrite(table_map)
    os.environ.setdefault("UNSAN_TEST_TABLE_MODE", "1")
    print("[UNSAN-TEST] SQL table routing:")
    for source, target in table_map.items():
        print(f"[UNSAN-TEST]   {source} -> {target}")
