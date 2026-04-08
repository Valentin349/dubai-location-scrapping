import json
import re
from abc import ABC, abstractmethod

import requests

TWOGIS_CATALOG_API = "https://catalog.api.2gis.com/3.0/items"
TWOGIS_KEY = "ruregt3044"


class BaseScraper(ABC):

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable scraper name."""

    @property
    @abstractmethod
    def output_file(self) -> str:
        """Absolute path to the JSON output file."""

    @abstractmethod
    def scrape(self) -> list[dict]:
        """Return normalised records.

        Each record must contain at minimum:
            name (str)  – venue/store name
            url  (str)  – canonical page URL

        Standard optional keys:
            phone          (str | None)
            floor          (str | None)  – human-readable floor level
            unit           (str | None)  – unit number
            content        (list | None) – structured content (e.g. Deliveroo instructions)
            map_url        (str | None)
            directions_url (str | None)

        Invariant enforced by run(): every record must have at least one of:
            - floor or unit        (location info)
            - map_url or directions_url  (navigation info)
            - content              (structured content)
        """

    @staticmethod
    def _query_2gis(name: str, lng: float, lat: float, radius: int = 500):
        """Query the 2GIS catalog API and return (lat, lon, short_id) or None."""
        try:
            r = requests.get(TWOGIS_CATALOG_API, params={
                "q": name,
                "point": f"{lng},{lat}",
                "radius": radius,
                "key": TWOGIS_KEY,
                "fields": "items.point",
                "locale": "en_AE",
                "type": "branch",
            }, timeout=10)
            data = r.json()
            items = (data.get("result") or {}).get("items") or []
            if not items:
                return None
            item = items[0]
            point = item.get("point", {})
            item_lat = point.get("lat")
            item_lon = point.get("lon")
            full_id = item.get("id", "")
            short_id = full_id.split("_")[0] if full_id else None
            if item_lat and item_lon and short_id:
                return item_lat, item_lon, short_id
        except Exception:
            pass
        return None

    @staticmethod
    def _build_2gis_url(lat: float, lon: float, short_id: str) -> str:
        return f"https://2gis.ae/dubai/directions/points/%7C{lon}%2C{lat}%3B{short_id}"

    def _validate_record(self, r: dict) -> None:
        has_location = r.get("floor") or r.get("unit")
        has_navigation = r.get("map_url") or r.get("directions_url")
        has_content = r.get("content")
        assert has_location or has_navigation or has_content, (
            f"[{self.name}] Record has no floor/unit, map_url/directions_url, or content:\n"
            f"{json.dumps(r, indent=2, ensure_ascii=False)}"
        )
        phone = r.get("phone")
        if phone is not None:
            assert re.fullmatch(r"\+\d+", phone), (
                f"[{self.name}] Invalid phone format '{phone}' in record:\n"
                f"{json.dumps(r, indent=2, ensure_ascii=False)}"
            )

    def run(self) -> None:
        results = []
        for r in self.scrape():
            self._validate_record(r)
            results.append(r)
        results.sort(key=lambda r: r.get("name", "").lower())
        with open(self.output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"Done. Saved {len(results)} records to {self.output_file}")
