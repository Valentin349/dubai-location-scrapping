import os
import sys
import time
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from baseScraper import BaseScraper


BASE_URL = "https://thedubaimall.com"
STORES_URL = f"{BASE_URL}/en/data/get-all-stores"

ABUZZ_EMBED = "https://map.abuzz.tech/amap/embedex.php"
ABUZZ_API_KEY = "AC6B0D339940194AB7E2BC8E1967A4AE"
ABUZZ_SITE = "TDM_"

DINE_CATEGORY_IDS = {101, 221, 224, 227, 281}

ZONE_MAP = {
    "LG":  "Lower Ground",
    "GF":  "Ground Floor",
    "FF":  "First Floor",
    "SF":  "Second Floor",
    "TF":  "Third Floor",
    "4F":  "Fourth Floor",
    "BF":  "Basement Floor",
    "BFM": "Basement Floor Mezzanine",
    "LGM": "Lower Ground Mezzanine",
    "TFM": "Third Floor Mezzanine",
    "GS":  "Gold Souk",
    "ML":  "Mall Level",
    "L1":  "Level 1",
    "L2":  "Souk Al Bahar L2",
    "L3":  "Souk Al Bahar L3",
}


class DubaiMallScraper(BaseScraper):

    @property
    def name(self):
        return "Dubai Mall"

    @property
    def output_file(self):
        return os.path.join(os.path.dirname(os.path.dirname(__file__)), "results", "dubai_mall_dine.json")

    @staticmethod
    def _parse_unit(unit_number):
        """Split 'TDM-GF-016' into ('Ground Floor', '016')."""
        if not unit_number:
            return None, None
        parts = unit_number.split("-")
        if len(parts) < 3:
            return None, unit_number
        floor_name = ZONE_MAP.get(parts[1], parts[1])
        unit = "-".join(parts[2:])
        return floor_name, unit

    @staticmethod
    def _build_map_url(unit_number):
        if not unit_number:
            return None
        return (
            f"{ABUZZ_EMBED}?site={ABUZZ_SITE}&apiKey={ABUZZ_API_KEY}"
            f"&searchUI=true&servicesList=true&baseui=true"
            f"&poiUI=true&pathUI=true&hover=true&lazyld=false&mobile=true"
            f"&node={unit_number}"
        )

    @staticmethod
    def _fetch_stores():
        response = requests.get(STORES_URL, timeout=15)
        response.raise_for_status()
        return response.json()

    def scrape(self):
        stores = self._fetch_stores()
        dine_stores = [
            s for s in stores
            if any(c.get("id") in DINE_CATEGORY_IDS for c in (s.get("categories") or []))
        ]
        print(f"Found {len(dine_stores)} dine stores.")

        results = []
        for i, store in enumerate(dine_stores, 1):
            name = store.get("name")
            unit_number = store.get("unitNumber")
            floor, unit = self._parse_unit(unit_number)

            parking = store.get("closestParkingEntrance") or {}
            lat = parking.get("latitude")
            lng = parking.get("longitude")

            twogis = self._query_2gis(name, lng, lat) if lat and lng else None
            directions_url = self._build_2gis_url(*twogis) if twogis else None
            time.sleep(0.15)
            print(f"  [{i}/{len(dine_stores)}] {name}")

            record = {
                "name": name,
                "url": f"{BASE_URL}/en/shop/{store.get('slug')}",
                "phone": store.get("phone"),
                "floor": floor,
                "unit": unit,
                "map_url": self._build_map_url(unit_number),
                "directions_url": directions_url,
            }
            results.append(record)
        return results


if __name__ == "__main__":
    DubaiMallScraper().run()
