from datetime import datetime, timedelta
from functools import lru_cache
from typing import Dict, List, Optional

from requests import request

DOMAIN = "https://recruitment.developers.emako.pl"
PAGES = [1, 2, 3]


class Connector:
    @lru_cache
    def headers(self) -> Dict[str, str]:
        return {
            "Authorization": None,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def pages_decorator(self, pages):
        def inner(func):
            def wrapper():
                for page in pages:
                    value = func(page)
                return value
            return wrapper
        return inner

    @pages_decorator(pages=PAGES)
    def request(self, method: str, path: str, page: int, data: dict = {}) -> dict:
        return request(
            method, f"{DOMAIN}/{path}?page={page}", json=data, headers=self.headers()
        ).json()

    def get_products(self, ids: Optional[List[int]] = None) -> List[dict]:
        return self.request("GET", "products", {"ids": ids})["result"]

    def get_all_products_summary(self) -> List[dict]:
        return self.request("GET", "products", {"detailed": False})["result"]

    def get_new_products(self, newer_than: Optional[datetime] = None) -> List[dict]:
        if newer_than is None:
            newer_than = datetime.now() - timedelta(days=5)
        return self.request(
            "GET", "products", {"created_at": {"start": newer_than.isoformat()}})["result"]

    def add_products(self, products: List[dict]):
        units = 20
        products_blocks = [products[i: i + units] for i in range(0, len(products), units)]
        for products in products_blocks:
            return self.request("POST", "products", {"products": products})["result"]

    def update_stocks(self, stocks: Dict[int, list]):
        current_data = self.get_products(list(stocks))
        for product_entry in current_data:
            product_entry["details"]["supply"] = stocks[product_entry["id"]]
        units = 20
        paginated_data = [current_data[i: i + units] for i in range(0, len(current_data), units)]
        for data in paginated_data:
            return self.request("PUT", "products", {"products": data})
