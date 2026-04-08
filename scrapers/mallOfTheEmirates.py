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
        return re.sub(r"%[0-9A-F]{2}", lambda hexMatch: hexMatch.group(0).lower(), encoded)

    def _try_get(self, url):
        for attempt in range(1, 6):
            try:
                response = self._session.get(url, timeout=15)
                if response.status_code == 403:
                    time.sleep(2 ** attempt)
                    continue

                response.raise_for_status()
                return response
            except requests.exceptions.RequestsError:
                return None
            except requests.exceptions.HTTPError:
                if attempt == 5:
                    return response
                time.sleep(2 ** attempt)
        return None

    def _fetch_directory_stores(self):
        response = self._session.get(DINE_URL, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        component = soup.find("mf-search-and-filter-store")

        if not component:
            raise RuntimeError("Could not find <mf-search-and-filter-store> on the directory page.")
        data = json.loads(html.unescape(component.get("searchandfilterstore", "")))

        return data.get("stores", [])

    def _scrape_store_detail(self, store):
        link = store.get("link", "")
        link = re.sub(r"---.*$", "", link)
        encodedLink = self._encode_url(link)
        urlAccented = encodedLink if encodedLink.startswith("http") else BASE_URL + encodedLink
        asciiLink = unicodedata.normalize("NFKD", link).encode("ascii", "ignore").decode("ascii")
        urlAscii = asciiLink if asciiLink.startswith("http") else BASE_URL + asciiLink

        result = {
            "name": store.get("title"),
            "url": urlAccented,
            "floor": store.get("level"),
            "unit": None,
            "phone": None,
            "floor_abbreviation": None,
            "nearest_parking": store.get("nearestparkingvalue"),
            "destination_id": None,
        }

        response = self._try_get(urlAccented)
        if response is None or response.status_code == 404:
            response = self._try_get(urlAscii)
            if response is not None and response.ok:
                result["url"] = urlAscii
        if response is None:
            result["error"] = "redirect_loop"
            return result
        if not response.ok:
            result["error"] = str(response.status_code)
            return result

        try:
            soup = BeautifulSoup(response.text, "html.parser")
            headerComponent = soup.find("mf-header-with-info")
            if headerComponent:
                rawAttribute = headerComponent.get("otherstores", "")
                if rawAttribute:
                    contactData = json.loads(html.unescape(rawAttribute))
                    if isinstance(contactData, list):
                        contactData = contactData[0] if contactData else {}
                    result["floor_abbreviation"] = contactData.get("FloorAbbreviation")
                    if contactData.get("FloorDetails"):
                        result["floor"] = contactData["FloorDetails"]
                    if contactData.get("NearestParking"):
                        result["nearest_parking"] = contactData["NearestParking"]
                    result["destination_id"] = contactData.get("DestinationId")
                    for contactEntry in contactData.get("ContactData") or []:
                        if contactEntry.get("Title", "").lower() == "phone":
                            rawPhone = contactEntry.get("Link", "")
                            result["phone"] = rawPhone.removeprefix("tel:")
                            break
        except Exception as error:
            result["error"] = str(error)

        return result

    @staticmethod
    def _build_map_url(destinationId):
        if not destinationId:
            return None
        return f"{BASE_URL}/en/map?destId={destinationId}"

    def scrape(self):
        stores = self._fetch_directory_stores()
        print(f"Found {len(stores)} stores.")

        results = []
        for index, store in enumerate(stores, 1):
            result = self._scrape_store_detail(store)
            result["map_url"] = self._build_map_url(result.pop("destination_id", None))
            twoGisResult = self._query_2gis(result["name"], MOE_LNG, MOE_LAT)
            result["directions_url"] = self._build_2gis_url(*twoGisResult) if twoGisResult else None
            results.append(result)
            time.sleep(0.15)
            print(f"  [{index}/{len(stores)}] {result['name']}")

        return results


if __name__ == "__main__":
    MallOfTheEmiratesScraper().run()
