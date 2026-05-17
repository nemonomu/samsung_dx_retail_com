"""Embedded payload discovery for rendered retail pages."""

import json
import re


class EmbeddedPayloadMapper:
    def extract(self, tree, html_text=None):
        payloads = {
            "json_ld": [],
            "next_data": [],
            "apollo_cache": [],
            "initial_state": [],
            "application_json": [],
        }

        for script in tree.xpath('//script[@type="application/ld+json"]/text()'):
            self._append_json(payloads["json_ld"], script)

        for script in tree.xpath('//script[@id="__NEXT_DATA__"]/text()'):
            self._append_json(payloads["next_data"], script)

        for script in tree.xpath('//script[@type="application/json"]/text()'):
            self._append_json(payloads["application_json"], script)

        html_text = html_text or ""
        for pattern, key in [
            (r"window\.__INITIAL_STATE__\s*=\s*({.*?})\s*;</script>", "initial_state"),
            (r"window\.__APOLLO_STATE__\s*=\s*({.*?})\s*;</script>", "apollo_cache"),
            (r"__APOLLO_STATE__\s*=\s*({.*?})", "apollo_cache"),
        ]:
            for match in re.finditer(pattern, html_text, re.DOTALL):
                self._append_json(payloads[key], match.group(1))

        return payloads

    def summarize_product_facts(self, payloads):
        facts = {}

        def walk(value):
            if isinstance(value, dict):
                item_type = value.get("@type")
                if item_type == "Product" or item_type == ["Product"]:
                    facts.setdefault("retailer_sku_name", value.get("name"))
                    aggregate = value.get("aggregateRating") or {}
                    if isinstance(aggregate, dict):
                        facts.setdefault("star_rating", aggregate.get("ratingValue"))
                        facts.setdefault("count_of_reviews", aggregate.get("reviewCount"))
                    offers = value.get("offers") or {}
                    if isinstance(offers, list) and offers:
                        offers = offers[0]
                    if isinstance(offers, dict):
                        price = offers.get("price")
                        if price and not str(price).startswith("$"):
                            price = f"${price}"
                        facts.setdefault("final_sku_price", price)
                        facts.setdefault("sku_status", offers.get("availability"))
                for child in value.values():
                    walk(child)
            elif isinstance(value, list):
                for child in value:
                    walk(child)

        for items in payloads.values():
            for item in items:
                walk(item)
        return facts

    @staticmethod
    def _append_json(target, text):
        try:
            target.append(json.loads(text))
        except Exception:
            return

