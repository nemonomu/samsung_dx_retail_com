"""Schema drift detection for API-first collectors."""

from crawler.discovery.graphql_mapper import schema_shape


def compare_schema(previous_schema, payload):
    current = schema_shape(payload)
    if previous_schema == current:
        return {"drift": False, "current_schema": current}
    return {"drift": True, "previous_schema": previous_schema, "current_schema": current}

