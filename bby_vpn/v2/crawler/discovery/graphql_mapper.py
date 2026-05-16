"""Persist GraphQL operations discovered through browser network interception."""

import json
import os
import re
from datetime import datetime

from .graphql_registry import GraphQLOperationRegistry


def _safe_name(value):
    value = value or "unknown"
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value)[:120]


def schema_shape(value, depth=0, max_depth=5):
    if depth >= max_depth:
        return type(value).__name__
    if isinstance(value, dict):
        return {str(k): schema_shape(v, depth + 1, max_depth) for k, v in list(value.items())[:80]}
    if isinstance(value, list):
        if not value:
            return []
        return [schema_shape(value[0], depth + 1, max_depth)]
    return type(value).__name__


class GraphQLMapper:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.map_dir = os.path.join(base_dir, "graphql_map")
        os.makedirs(self.map_dir, exist_ok=True)
        self.registry = GraphQLOperationRegistry(base_dir)
        self.sku_map_path = os.path.join(base_dir, "graphql_sku_map.json")

    def record(self, operation_name, endpoint_url, request_payload, request_headers, response_body, cookies=None):
        operation_name = operation_name or "unknown"
        payload = {
            "operationName": operation_name,
            "endpoint_url": endpoint_url,
            "request_payload": request_payload,
            "request_headers": request_headers or {},
            "response_schema": schema_shape(response_body),
            "sample_response": response_body,
            "required_cookies": sorted((cookies or {}).keys()),
            "variables": request_payload.get("variables") if isinstance(request_payload, dict) else None,
            "captured_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        }
        path = os.path.join(self.map_dir, f"graphql_operation_{_safe_name(operation_name)}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=str)

        self.registry.upsert(operation_name, endpoint_url, request_payload, request_headers)
        self._record_sku_map(endpoint_url, request_payload, request_headers, response_body)
        return path

    def _record_sku_map(self, endpoint_url, request_payload, request_headers, response_body):
        try:
            headers = request_headers or {}
            referer = headers.get("Referer") or headers.get("referer")
            variables = request_payload.get("variables") if isinstance(request_payload, dict) else {}
            sku_id = variables.get("skuId") if isinstance(variables, dict) else None
            if not referer or not sku_id:
                return

            try:
                with open(self.sku_map_path, encoding="utf-8") as f:
                    sku_map = json.load(f)
            except Exception:
                sku_map = {}

            sku_map[referer] = {
                "skuId": str(sku_id),
                "endpoint_url": endpoint_url,
                "operationName": request_payload.get("operationName") if isinstance(request_payload, dict) else None,
                "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            }
            with open(self.sku_map_path, "w", encoding="utf-8") as f:
                json.dump(sku_map, f, ensure_ascii=False, indent=2, default=str)
        except Exception:
            return
