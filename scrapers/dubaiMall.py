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
    def _parse_unit(unitNumber):
        """Split 'TDM-GF-016' into ('Ground Floor', '016')."""
        if not unitNumber:
            return None, None
        parts = unitNumber.split("-")

        if len(parts) < 3:
            return None, unitNumber
        
        floorName = ZONE_MAP.get(parts[1], parts[1])
        unit = "-".join(parts[2:])
        return floorName, unit

    @staticmethod
    def _build_map_url(unitNumber):
        if not unitNumber:
            return None
        return (
            f"{ABUZZ_EMBED}?site={ABUZZ_SITE}&apiKey={ABUZZ_API_KEY}"
            f"&searchUI=true&servicesList=true&baseui=true"
            f"&poiUI=true&pathUI=true&hover=true&lazyld=false&mobile=true"
            f"&node={unitNumber}"
        )

    @staticmethod
    def _fetch_stores():
        response = requests.get(STORES_URL, timeout=15)
        response.raise_for_status()
        return response.json()

    def scrape(self):
        allStores = self._fetch_stores()
        dineStores = [
            store for store in allStores
            if any(category.get("id") in DINE_CATEGORY_IDS for category in (store.get("categories") or []))
        ]
        print(f"Found {len(dineStores)} dine stores.")

        results = []
        for index, store in enumerate(dineStores, 1):
            storeName = store.get("name")
            unitNumber = store.get("unitNumber")
            floor, unit = self._parse_unit(unitNumber)

            parkingEntrance = store.get("closestParkingEntrance") or {}
            latitude = parkingEntrance.get("latitude")
            longitude = parkingEntrance.get("longitude")

            twoGisResult = self._query_2gis(storeName, longitude, latitude) if latitude and longitude else None
            directionsUrl = self._build_2gis_url(*twoGisResult) if twoGisResult else None
            time.sleep(0.15)
            print(f"  [{index}/{len(dineStores)}] {storeName}")

            results.append({
                "name": storeName,
                "url": f"{BASE_URL}/en/shop/{store.get('slug')}",
                "phone": store.get("phone"),
                "floor": floor,
                "unit": unit,
                "map_url": self._build_map_url(unitNumber),
                "directions_url": directionsUrl,
            })
        return results


if __name__ == "__main__":
    DubaiMallScraper().run()
