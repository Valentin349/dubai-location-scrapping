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
        r = requests.post(f"{POINTR_API}/auth/token", json={
            "client_id": POINTR_CLIENT_ID,
            "client_secret": POINTR_CLIENT_SECRET,
            "grant_type": "client_credentials",
        }, headers={"Content-Type": "application/json"}, timeout=15)
        r.raise_for_status()
        return r.json()["result"]["access_token"]

    def _fetch_pointr_pois(self, token):
        r = requests.get(
            f"{POINTR_API}/sites/{POINTR_SITE_ID}/pois",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        r.raise_for_status()
        features = r.json()["result"]["features"]

        poi_by_eid = {}
        for f in features:
            props = f["properties"]
            eid = props.get("eid")
            if not eid:
                continue
            geom = f.get("geometry")
            centroid = None
            if geom and geom.get("type") == "Polygon":
                coords = geom["coordinates"][0]
                lngs = [c[0] for c in coords]
                lats = [c[1] for c in coords]
                centroid = {
                    "lat": round(sum(lats) / len(lats), 8),
                    "lng": round(sum(lngs) / len(lngs), 8),
                }
            poi_by_eid[eid] = {
                "fid": props.get("fid"),
                "floor": LEVEL_MAP.get(props.get("lvl"), FLOOR_CODE_MAP.get(
                    eid.split("-")[1] if len(eid.split("-")) > 1 else "", ""
                )),
                "centroid": centroid,
            }
        return poi_by_eid

    def _get_store_links(self):
        resp = self._session.get(DINE_URL, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        stores = []
        for a in soup.select("a.ctaDetailsBtn"):
            href = a.get("href", "")
            full_url = href if href.startswith("http") else BASE_URL + href
            card_footer = a.find_parent(class_="cardFooter")
            name_tag = card_footer.find("p") if card_footer else None
            name = name_tag.get_text(strip=True) if name_tag else full_url.split("/")[-2].replace("-", " ").title()
            stores.append({"name": name, "url": full_url})
        return stores

    @staticmethod
    def _parse_store_code(code):
        if not code:
            return None, None
        parts = code.split("-")
        if len(parts) < 3:
            return None, None
        floor_name = FLOOR_CODE_MAP.get(parts[1], parts[1])
        unit = "-".join(parts[2:])
        return floor_name, unit

    def _scrape_store(self, store):
        url = store["url"]
        try:
            resp = self._session.get(url, timeout=15)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            store_code = None
            m = re.search(r"var\s+ShopToSearch\s*=\s*'([^']+)'", resp.text)
            if m:
                store_code = m.group(1)

            poi_fid = None
            m2 = re.search(r"ptrHighlightPoiIdentifier=([\w-]+)", resp.text)
            if m2:
                poi_fid = m2.group(1)

            floor_from_code, unit = self._parse_store_code(store_code)

            parking = None
            parking_tag = soup.find(class_="neerest-parking")
            if parking_tag:
                parking = re.sub(r"nearest parking[:\s]*", "", parking_tag.get_text(strip=True), flags=re.I).strip()

            phone = None
            phone_tag = soup.find("a", href=re.compile(r"^tel:"))
            if phone_tag:
                phone = phone_tag.get_text(strip=True)

            return {
                "name": store["name"],
                "url": url,
                "store_code": store_code,
                "_poi_fid": poi_fid,
                "floor": floor_from_code,
                "unit": unit,
                "nearest_parking": parking,
                "phone": phone,
            }

        except Exception as e:
            return {
                "name": store["name"],
                "url": url,
                "error": str(e),
            }

    def scrape(self):
        token = self._get_pointr_token()
        poi_map = self._fetch_pointr_pois(token)
        print(f"Loaded {len(poi_map)} POIs from Pointr.")

        stores = self._get_store_links()
        print(f"Found {len(stores)} dine stores.")

        results = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(self._scrape_store, s): s for s in stores}
            for future in as_completed(futures):
                results.append(future.result())

        for i, result in enumerate(results, 1):
            store_code = result.get("store_code")
            poi_fid = result.pop("_poi_fid", None)
            centroid = None

            if store_code and store_code in poi_map:
                poi = poi_map[store_code]
                centroid = poi["centroid"]
                if poi["floor"] and not result.get("floor"):
                    result["floor"] = poi["floor"]

            result["map_url"] = (
                f"https://dubaihills.pointr.cloud/websdk.html"
                f"?ptrSiteInternalIdentifier=1"
                f"&ptrHighlightPoiIdentifier={poi_fid}"
            ) if poi_fid else None

            twogis = self._query_2gis(result["name"], centroid["lng"], centroid["lat"], radius=300) if centroid else None
            if twogis:
                result["directions_url"] = self._build_2gis_url(*twogis)
            elif centroid:
                result["directions_url"] = (
                    f"https://2gis.ae/dubai/directions/points/"
                    f"%7C{centroid['lng']}%2C{centroid['lat']}"
                )
            else:
                result["directions_url"] = None

            print(f"  [{i}/{len(results)}] {result['name']}")
            time.sleep(0.15)

        return results


if __name__ == "__main__":
    DubaiHillsMallScraper().run()
