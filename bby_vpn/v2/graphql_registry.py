"""GraphQL operation registry persisted as JSON."""

import json
import os
from datetime import datetime


class GraphQLOperationRegistry:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        os.makedirs(base_dir, exist_ok=True)
        self.registry_path = os.path.join(base_dir, "graphql_registry.json")
        self.operations = self._load()

    def _load(self):
        if not os.path.exists(self.registry_path):
            return {}
        try:
            with open(self.registry_path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    def save(self):
        with open(self.registry_path, "w", encoding="utf-8") as f:
            json.dump(self.operations, f, ensure_ascii=False, indent=2, default=str)

    def upsert(self, operation_name, endpoint_url, request_payload, request_headers=None):
        variables = request_payload.get("variables") if isinstance(request_payload, dict) else None
        persisted_hash = None
        try:
            persisted_hash = (
                request_payload.get("extensions", {})
                .get("persistedQuery", {})
                .get("sha256Hash")
            )
        except Exception:
            persisted_hash = None

        self.operations[operation_name] = {
            "operationName": operation_name,
            "endpoint_url": endpoint_url,
            "request_template": request_payload,
            "required_variables": sorted((variables or {}).keys()) if isinstance(variables, dict) else [],
            "persisted_query_hash": persisted_hash,
            "request_headers": request_headers or {},
            "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        }
        self.save()
        return self.operations[operation_name]

