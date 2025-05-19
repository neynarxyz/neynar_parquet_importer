from functools import cache
from cachetools import TTLCache, cachedmethod
import requests


class NeynarApiClient:
    @cache
    @staticmethod
    def new(api_key: str, api_url: str):
        return NeynarApiClient(
            api_key=api_key,
            api_url=api_url,
        )

    def __init__(self, api_key: str | None, api_url: str):
        self.api_url = api_url.rstrip("/")

        self.cache = TTLCache(maxsize=1, ttl=60)

        self.session = requests.Session()

        if not api_key:
            raise ValueError("API key is required")

        self.session.headers.update({"X-Api-Key": api_key})

    def get(self, endpoint: str):
        url = f"{self.api_url}/{endpoint}"

        response = self.session.get(url)

        if response.status_code != 200:
            raise Exception(f"Failed to get {endpoint}: {response.text}")

        return response.json()

    @cachedmethod(lambda self: self.cache)
    def get_portal_pricing(self, product: str) -> dict[str, int]:
        raw_json = self.get("/portal/pricing")

        try:
            product_pricing = raw_json["pricing"]["products"]["data"][product][product]
        except KeyError:
            raise KeyError(
                f"Product {product} not found in pricing data. Valid products: {list(raw_json['pricing']['products']['data'].keys())}"
            )

        return {key: value["price"]["amount"] for key, value in product_pricing.items()}
