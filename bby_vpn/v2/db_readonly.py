"""Read-only PostgreSQL connection helper for V2 crawlers."""

import psycopg2


READONLY_OPTIONS = "-c default_transaction_read_only=on"


def connect_readonly(db_config):
    """Open a PostgreSQL connection that refuses writes at the server level."""
    config = dict(db_config)
    existing_options = config.get("options")
    if existing_options:
        config["options"] = f"{existing_options} {READONLY_OPTIONS}"
    else:
        config["options"] = READONLY_OPTIONS

    conn = psycopg2.connect(**config)
    conn.autocommit = True
    try:
        with conn.cursor() as cursor:
            cursor.execute("SET default_transaction_read_only = on")
            cursor.execute("SET transaction_read_only = on")
    except Exception:
        # transaction_read_only can fail outside an active transaction on some
        # setups; default_transaction_read_only still protects subsequent work.
        pass
    return conn
