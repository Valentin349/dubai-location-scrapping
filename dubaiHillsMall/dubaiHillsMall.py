import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://www.dubaihillsmall.ae"
DINE_URL = f"{BASE_URL}/dine/"

POINTR_API = "https://dubaihills-api.pointr.cloud/api/v8"
POINTR_CLIENT_ID = "ed41559d-f9fd-45cf-9e43-3157f7255570"
POINTR_CLIENT_SECRET = "f17d6b38-f2da-4d9c-b760-0edc4fb4569c"
POINTR_SITE_ID = 1

TWOGIS_CATALOG_API = "https://catalog.api.2gis.com/3.0/items"
TWOGIS_KEY = "ruregt3044"

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

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
})


def get_pointr_token():
    r = requests.post(f"{POINTR_API}/auth/token", json={
        "client_id": POINTR_CLIENT_ID,
        "client_secret": POINTR_CLIENT_SECRET,
        "grant_type": "client_credentials",
    }, headers={"Content-Type": "application/json"}, timeout=15)
    r.raise_for_status()
    return r.json()["result"]["access_token"]


def fetch_pointr_pois(token):
    """Fetch all POIs from Pointr and return a dict keyed by eid (store code)."""
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


def lookup_2gis(name, centroid):
    """
    Search 2GIS catalog for a store by name near the Pointr centroid.
    Returns (lat, lon, short_id) or None if not found.
    short_id is the numeric prefix of the 2GIS item ID used in direction URLs.
    """
    if not centroid:
        return None
    try:
        r = requests.get(TWOGIS_CATALOG_API, params={
            "q": name,
            "point": f"{centroid['lng']},{centroid['lat']}",
            "radius": 300,
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
        lat = point.get("lat")
        lon = point.get("lon")
        full_id = item.get("id", "")
        short_id = full_id.split("_")[0] if full_id else None
        if lat and lon and short_id:
            return lat, lon, short_id
    except Exception:
        pass
    return None


def get_store_links():
    """Fetch the main dine page and extract all store name + detail URLs."""
    resp = SESSION.get(DINE_URL, timeout=15)
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


def parse_store_code(code):
    if not code:
        return None, None
    parts = code.split("-")
    if len(parts) < 3:
        return None, None
    floor_code = parts[1]
    unit = "-".join(parts[2:])
    floor_name = FLOOR_CODE_MAP.get(floor_code, floor_code)
    return floor_name, unit


def scrape_store(store):
    """Fetch a store detail page and extract location data."""
    url = store["url"]
    try:
        resp = SESSION.get(url, timeout=15)
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

        floor_from_code, unit = parse_store_code(store_code)

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


def main():
    print("Fetching Pointr POI data...")
    token = get_pointr_token()
    poi_map = fetch_pointr_pois(token)
    print(f"Loaded {len(poi_map)} POIs from Pointr API.")

    print("Fetching store list from Dubai Hills Mall dine page...")
    stores = get_store_links()
    print(f"Found {len(stores)} dine stores.")

    print("Scraping store detail pages...")
    results = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(scrape_store, s): s for s in stores}
        for future in as_completed(futures):
            results.append(future.result())

    # Enrich with Pointr + 2GIS data sequentially (rate-limit friendly)
    print("Looking up 2GIS coordinates and IDs...")
    for i, result in enumerate(results, 1):
        store_code = result.get("store_code")
        poi_fid = result.pop("_poi_fid", None)
        centroid = None

        if store_code and store_code in poi_map:
            poi = poi_map[store_code]
            centroid = poi["centroid"]
            if poi["floor"] and not result.get("floor"):
                result["floor"] = poi["floor"]

        # Pointr interactive map URL
        if poi_fid:
            result["map_url"] = (
                f"https://dubaihills.pointr.cloud/websdk.html"
                f"?ptrSiteInternalIdentifier=1"
                f"&ptrHighlightPoiIdentifier={poi_fid}"
            )
        else:
            result["map_url"] = None

        # 2GIS: look up exact coordinates + object ID
        twogis = lookup_2gis(result["name"], centroid)
        if twogis:
            lat, lon, short_id = twogis
            result["directions_url"] = (
                f"https://2gis.ae/dubai/directions/points/"
                f"%7C{lon}%2C{lat}%3B{short_id}"
            )
            print(f"  [{i}/{len(results)}] {result['name']} — found: {short_id}")
        elif centroid:
            # Fallback: Pointr centroid without 2GIS ID
            result["directions_url"] = (
                f"https://2gis.ae/dubai/directions/points/"
                f"%7C{centroid['lng']}%2C{centroid['lat']}"
            )
            print(f"  [{i}/{len(results)}] {result['name']} — fallback coords (no 2GIS ID)")
        else:
            result["directions_url"] = None
            print(f"  [{i}/{len(results)}] {result['name']} — no location data")

        time.sleep(0.15)  # polite delay for 2GIS API

    results.sort(key=lambda x: x.get("name", "").lower())

    with_2gis = sum(1 for r in results if r.get("directions_url") and "%3B" in (r.get("directions_url") or ""))
    with_fallback = sum(1 for r in results if r.get("directions_url") and "%3B" not in (r.get("directions_url") or ""))
    print(f"\n{with_2gis} stores with 2GIS ID, {with_fallback} with fallback coords, "
          f"{len(results) - with_2gis - with_fallback} with no URL.")

    output_path = os.path.join(os.path.dirname(__file__), "dubai_hills_mall_dine.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(results)} stores to {output_path}")


if __name__ == "__main__":
    main()
