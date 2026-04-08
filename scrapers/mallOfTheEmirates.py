import html
import json
import os
import re
import sys
import time
import unicodedata
from urllib.parse import quote

from curl_cffi import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from baseScraper import BaseScraper


BASE_URL = "https://www.malloftheemirates.com"
DINE_URL = f"{BASE_URL}/en/dining-directory"

MOE_LAT = 25.1181
MOE_LNG = 55.2005


class MallOfTheEmiratesScraper(BaseScraper):

    def __init__(self):
        self._session = requests.Session(impersonate="chrome120")

    @property
    def name(self):
        return "Mall of the Emirates"

    @property
    def output_file(self):
        return os.path.join(os.path.dirname(os.path.dirname(__file__)), "results", "mall_of_the_emirates_dine.json")

    @staticmethod
    def _encode_url(link):
        encoded = quote(link, safe="/:@!$&'()*+,;=")
        return re.sub(r"%[0-9A-F]{2}", lambda m: m.group(0).lower(), encoded)

    def _fetch_directory_stores(self):
        resp = self._session.get(DINE_URL, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        component = soup.find("mf-search-and-filter-store")
        if not component:
            raise RuntimeError("Could not find <mf-search-and-filter-store> on the directory page.")
        data = json.loads(html.unescape(component.get("searchandfilterstore", "")))
        return data.get("stores", [])

    def _scrape_store_detail(self, store):
        link = store.get("link", "")
        link = re.sub(r"---.*$", "", link)
        encoded_link = self._encode_url(link)
        url_accented = encoded_link if encoded_link.startswith("http") else BASE_URL + encoded_link
        ascii_link = unicodedata.normalize("NFKD", link).encode("ascii", "ignore").decode("ascii")
        url_ascii = ascii_link if ascii_link.startswith("http") else BASE_URL + ascii_link

        result = {
            "name": store.get("title"),
            "url": url_accented,
            "floor": store.get("level"),
            "unit": None,
            "phone": None,
            "floor_abbreviation": None,
            "nearest_parking": store.get("nearestparkingvalue"),
            "destination_id": None,
        }

        def _try_get(url):
            for attempt in range(1, 6):
                try:
                    resp = self._session.get(url, timeout=15)
                    if resp.status_code == 403:
                        time.sleep(2 ** attempt)
                        continue
                    resp.raise_for_status()
                    return resp
                except requests.exceptions.RequestsError:
                    return None
                except requests.exceptions.HTTPError:
                    if attempt == 5:
                        return resp
                    time.sleep(2 ** attempt)
            return None

        resp = _try_get(url_accented)
        if resp is None or resp.status_code == 404:
            resp = _try_get(url_ascii)
            if resp is not None and resp.ok:
                result["url"] = url_ascii
        if resp is None:
            result["error"] = "redirect_loop"
            return result
        if not resp.ok:
            result["error"] = str(resp.status_code)
            return result

        try:
            soup = BeautifulSoup(resp.text, "html.parser")
            header = soup.find("mf-header-with-info")
            if header:
                raw = header.get("otherstores", "")
                if raw:
                    contact_data = json.loads(html.unescape(raw))
                    if isinstance(contact_data, list):
                        contact_data = contact_data[0] if contact_data else {}
                    result["floor_abbreviation"] = contact_data.get("FloorAbbreviation")
                    if contact_data.get("FloorDetails"):
                        result["floor"] = contact_data["FloorDetails"]
                    if contact_data.get("NearestParking"):
                        result["nearest_parking"] = contact_data["NearestParking"]
                    result["destination_id"] = contact_data.get("DestinationId")
                    for entry in contact_data.get("ContactData") or []:
                        if entry.get("Title", "").lower() == "phone":
                            raw_phone = entry.get("Link", "")
                            result["phone"] = raw_phone.removeprefix("tel:")
                            break
        except Exception as e:
            result["error"] = str(e)

        return result

    @staticmethod
    def _build_map_url(destination_id):
        if not destination_id:
            return None
        return f"{BASE_URL}/en/map?destId={destination_id}"

    def scrape(self):
        stores = self._fetch_directory_stores()
        print(f"Found {len(stores)} stores.")

        results = []
        for i, store in enumerate(stores, 1):
            result = self._scrape_store_detail(store)
            result["map_url"] = self._build_map_url(result.pop("destination_id", None))
            twogis = self._query_2gis(result["name"], MOE_LNG, MOE_LAT)
            result["directions_url"] = self._build_2gis_url(*twogis) if twogis else None
            results.append(result)
            time.sleep(0.15)
            print(f"  [{i}/{len(stores)}] {result['name']}")

        return results


if __name__ == "__main__":
    MallOfTheEmiratesScraper().run()
