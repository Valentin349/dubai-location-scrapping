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
            - floor or unit                    (location info)
            - map_url or directions_url        (navigation info)
            - content                          (structured content)
        """

    @staticmethod
    def _query_2gis(name: str, lng: float, lat: float, radius: int = 500):
        """Query the 2GIS catalog API and return (lat, lon, shortId) or None."""
        try:
            response = requests.get(TWOGIS_CATALOG_API, params={
                "q": name,
                "point": f"{lng},{lat}",
                "radius": radius,
                "key": TWOGIS_KEY,
                "fields": "items.point",
                "locale": "en_AE",
                "type": "branch",
            }, timeout=10)
            responseData = response.json()
            items = (responseData.get("result") or {}).get("items") or []

            if not items:
                return None
            
            firstItem = items[0]
            itemPoint = firstItem.get("point", {})
            itemLat = itemPoint.get("lat")
            itemLon = itemPoint.get("lon")
            fullId = firstItem.get("id", "")
            shortId = fullId.split("_")[0] if fullId else None
            
            if itemLat and itemLon and shortId:
                return itemLat, itemLon, shortId
        except Exception:
            pass
        return None

    @staticmethod
    def _build_2gis_url(lat: float, lon: float, shortId: str) -> str:
        return f"https://2gis.ae/dubai/directions/points/%7C{lon}%2C{lat}%3B{shortId}"

    def _validate_record(self, record: dict) -> None:
        hasLocation = record.get("floor") or record.get("unit")
        hasNavigation = record.get("map_url") or record.get("directions_url")
        hasContent = record.get("content")

        assert (hasLocation and hasNavigation) or hasContent, (
            f"[{self.name}] Record has no floor/unit, map_url/directions_url, or content:\n"
            f"{json.dumps(record, indent=2, ensure_ascii=False)}"
        )
        phone = record.get("phone")

        if phone is not None:
            assert re.fullmatch(r"\+\d+", phone), (
                f"[{self.name}] Invalid phone format '{phone}' in record:\n"
                f"{json.dumps(record, indent=2, ensure_ascii=False)}"
            )

    def run(self) -> None:
        results = []
        for record in self.scrape():
            self._validate_record(record)
            results.append(record)

        results.sort(key=lambda record: record.get("name", "").lower())
        with open(self.output_file, "w", encoding="utf-8") as outputFile:
            json.dump(results, outputFile, indent=2, ensure_ascii=False)
        print(f"Done. Saved {len(results)} records to {self.output_file}")
