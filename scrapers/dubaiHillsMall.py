import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from baseScraper import BaseScraper


BASE_URL = "https://www.dubaihillsmall.ae"
DINE_URL = f"{BASE_URL}/dine/"

POINTR_API = "https://dubaihills-api.pointr.cloud/api/v8"
POINTR_CLIENT_ID = "ed41559d-f9fd-45cf-9e43-3157f7255570"
POINTR_CLIENT_SECRET = "f17d6b38-f2da-4d9c-b760-0edc4fb4569c"
POINTR_SITE_ID = 1

LEVEL_MAP = {
    -1: "Lower Ground / P1",
    0:  "Ground Floor",
    2:  "First Floor",
}

FLOOR_CODE_MAP = {
    "GF":  "Ground Floor",
    "FF":  "First Floor",
    "LG":  "Lower Ground",
    "SF":  "Second Floor",
    "TF":  "Third Floor",
    "BF":  "Basement Floor",
    "L1":  "Level 1",
    "L2":  "Level 2",
    "L3":  "Level 3",
}


class DubaiHillsMallScraper(BaseScraper):

    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    @property
    def name(self):
        return "Dubai Hills Mall"

    @property
    def output_file(self):
        return os.path.join(os.path.dirname(os.path.dirname(__file__)), "results", "dubai_hills_mall_dine.json")

    def _get_pointr_token(self):
        response = requests.post(f"{POINTR_API}/auth/token", json={
            "client_id": POINTR_CLIENT_ID,
            "client_secret": POINTR_CLIENT_SECRET,
            "grant_type": "client_credentials",
        }, headers={"Content-Type": "application/json"}, timeout=15)
        response.raise_for_status()

        return response.json()["result"]["access_token"]

    def _fetch_pointr_pois(self, accessToken):
        response = requests.get(
            f"{POINTR_API}/sites/{POINTR_SITE_ID}/pois",
            headers={"Authorization": f"Bearer {accessToken}"},
            timeout=30,
        )
        response.raise_for_status()
        features = response.json()["result"]["features"]

        poiByEid = {}
        for feature in features:
            properties = feature["properties"]
            eid = properties.get("eid")

            if not eid:
                continue

            geometry = feature.get("geometry")
            centroid = None

            if geometry and geometry.get("type") == "Polygon":
                coords = geometry["coordinates"][0]
                longitudes = [coordinate[0] for coordinate in coords]
                latitudes = [coordinate[1] for coordinate in coords]
                centroid = {
                    "lat": round(sum(latitudes) / len(latitudes), 8),
                    "lng": round(sum(longitudes) / len(longitudes), 8),
                }

            poiByEid[eid] = {
                "fid": properties.get("fid"),
                "floor": LEVEL_MAP.get(properties.get("lvl"), FLOOR_CODE_MAP.get(
                    eid.split("-")[1] if len(eid.split("-")) > 1 else "", ""
                )),
                "centroid": centroid,
            }
        return poiByEid

    def _get_store_links(self):
        response = self._session.get(DINE_URL, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        stores = []
        for buttonNode in soup.select("a.ctaDetailsBtn"):
            href = buttonNode.get("href", "")
            fullUrl = href if href.startswith("http") else BASE_URL + href
            cardFooter = buttonNode.find_parent(class_="cardFooter")
            nameTag = cardFooter.find("p") if cardFooter else None
            storeName = nameTag.get_text(strip=True) if nameTag else fullUrl.split("/")[-2].replace("-", " ").title()
            stores.append({"name": storeName, "url": fullUrl})
        return stores

    @staticmethod
    def _parse_store_code(storeCode):
        if not storeCode:
            return None, None
        
        parts = storeCode.split("-")
        if len(parts) < 3:
            return None, None
        
        floorName = FLOOR_CODE_MAP.get(parts[1], parts[1])
        unit = "-".join(parts[2:])
        return floorName, unit

    def _scrape_store(self, store):
        url = store["url"]
        try:
            response = self._session.get(url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            storeCode = None
            storeCodeMatch = re.search(r"var\s+ShopToSearch\s*=\s*'([^']+)'", response.text)
            if storeCodeMatch:
                storeCode = storeCodeMatch.group(1)

            poiFid = None
            poiFidMatch = re.search(r"ptrHighlightPoiIdentifier=([\w-]+)", response.text)
            if poiFidMatch:
                poiFid = poiFidMatch.group(1)

            floorFromCode, unit = self._parse_store_code(storeCode)

            nearestParking = None
            parkingTag = soup.find(class_="neerest-parking")
            if parkingTag:
                nearestParking = re.sub(r"nearest parking[:\s]*", "", parkingTag.get_text(strip=True), flags=re.I).strip()

            phone = None
            phoneTag = soup.find("a", href=re.compile(r"^tel:"))
            if phoneTag:
                phone = phoneTag.get_text(strip=True)

            return {
                "name": store["name"],
                "url": url,
                "store_code": storeCode,
                "_poi_fid": poiFid,
                "floor": floorFromCode,
                "unit": unit,
                "nearest_parking": nearestParking,
                "phone": phone,
            }

        except Exception as error:
            return {
                "name": store["name"],
                "url": url,
                "error": str(error),
            }

    def scrape(self):
        accessToken = self._get_pointr_token()
        poiMap = self._fetch_pointr_pois(accessToken)
        print(f"Loaded {len(poiMap)} POIs from Pointr.")

        stores = self._get_store_links()
        print(f"Found {len(stores)} dine stores.")

        results = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(self._scrape_store, store): store for store in stores}
            for future in as_completed(futures):
                results.append(future.result())

        for index, result in enumerate(results, 1):
            storeCode = result.get("store_code")
            poiFid = result.pop("_poi_fid", None)
            centroid = None

            if storeCode and storeCode in poiMap:
                poiEntry = poiMap[storeCode]
                centroid = poiEntry["centroid"]
                if poiEntry["floor"] and not result.get("floor"):
                    result["floor"] = poiEntry["floor"]

            result["map_url"] = (
                f"https://dubaihills.pointr.cloud/websdk.html"
                f"?ptrSiteInternalIdentifier=1"
                f"&ptrHighlightPoiIdentifier={poiFid}"
            ) if poiFid else None

            twoGisResult = self._query_2gis(result["name"], centroid["lng"], centroid["lat"], radius=300) if centroid else None
            if twoGisResult:
                result["directions_url"] = self._build_2gis_url(*twoGisResult)
            elif centroid:
                result["directions_url"] = (
                    f"https://2gis.ae/dubai/directions/points/"
                    f"%7C{centroid['lng']}%2C{centroid['lat']}"
                )
            else:
                result["directions_url"] = None

            print(f"  [{index}/{len(results)}] {result['name']}")
            time.sleep(0.15)

        return results


if __name__ == "__main__":
    DubaiHillsMallScraper().run()
