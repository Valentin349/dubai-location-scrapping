import html
import json
import os
import re
import time
import unicodedata
from urllib.parse import quote

from curl_cffi import requests
from bs4 import BeautifulSoup


BASE_URL = "https://www.malloftheemirates.com"
DINE_URL = f"{BASE_URL}/en/dining-directory"

TWOGIS_CATALOG_API = "https://catalog.api.2gis.com/3.0/items"
TWOGIS_KEY = "ruregt3044"

# Mall of the Emirates centroid — used as 2GIS search anchor for all stores
MOE_LAT = 25.1181
MOE_LNG = 55.2005

def _encode_url(link):
    """Percent-encode a link path with lowercase hex (e.g. é → %c3%a9)."""
    encoded = quote(link, safe="/:@!$&'()*+,;=")
    return re.sub(r"%[0-9A-F]{2}", lambda m: m.group(0).lower(), encoded)


SESSION = requests.Session(impersonate="chrome120")


def fetch_directory_stores():
    """Fetch the dining directory page and extract all store entries from embedded JSON."""
    resp = SESSION.get(DINE_URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    component = soup.find("mf-search-and-filter-store")
    if not component:
        raise RuntimeError("Could not find <mf-search-and-filter-store> on the directory page.")

    raw_attr = component.get("searchandfilterstore", "")
    data = json.loads(html.unescape(raw_attr))
    return data.get("stores", [])


def scrape_store_detail(store):
    """Fetch a store detail page and extract phone, floor, nearest parking, and map destination ID."""
    link = store.get("link", "")
    # Remove the "---<extra>" suffix the CMS appends to slugs.
    link = re.sub(r"---.*$", "", link)
    # Build accented URL with lowercase percent-encoding (e.g. é → %c3%a9).
    encoded_link = _encode_url(link)
    url_accented = encoded_link if encoded_link.startswith("http") else BASE_URL + encoded_link
    # ASCII fallback: strip accents (é → e) for stores whose slug the server
    # canonicalises without the accent (e.g. aspen-café → aspen-cafe).
    ascii_link = unicodedata.normalize("NFKD", link).encode("ascii", "ignore").decode("ascii")
    url_ascii = ascii_link if ascii_link.startswith("http") else BASE_URL + ascii_link

    result = {
        "name": store.get("title"),
        "url": url_accented,
        "level": store.get("level"),
        "phone": None,
        "floor_abbreviation": None,
        "nearest_parking": store.get("nearestparkingvalue"),
        "destination_id": None,
    }

    def _try_get(url):
        for attempt in range(1, 6):
            try:
                resp = SESSION.get(url, timeout=15)
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
        # Accented slug has redirect loop or doesn't exist — try ASCII variant
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
                print(json.dumps(contact_data, indent=2))
                if isinstance(contact_data, list):
                    contact_data = contact_data[0] if contact_data else {}
                result["floor_abbreviation"] = contact_data.get("FloorAbbreviation")
                if contact_data.get("FloorDetails"):
                    result["level"] = contact_data["FloorDetails"]
                if contact_data.get("NearestParking"):
                    result["nearest_parking"] = contact_data["NearestParking"]
                result["destination_id"] = contact_data.get("DestinationId")

                for entry in contact_data.get("ContactData") or []:
                    if entry.get("Title", "").lower() == "phone":
                        result["phone"] = entry.get("Link")
                        break

    except Exception as e:
        result["error"] = str(e)

    return result


def build_map_url(destination_id):
    if not destination_id:
        return None
    return f"{BASE_URL}/en/map?destId={destination_id}"


def lookup_2gis(name):
    """Search 2GIS by name anchored to Mall of the Emirates coordinates."""
    try:
        r = requests.get(TWOGIS_CATALOG_API, params={
            "q": name,
            "point": f"{MOE_LNG},{MOE_LAT}",
            "radius": 500,
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
            return f"https://2gis.ae/dubai/directions/points/%7C{lon}%2C{lat}%3B{short_id}"
    except Exception:
        pass
    return None


def main():
    print("Fetching dining directory from Mall of the Emirates...")
    stores = fetch_directory_stores()
    print(f"Found {len(stores)} dining stores.")

    print("Scraping store detail pages and looking up 2GIS URLs...")
    results = []
    for i, store in enumerate(stores, 1):
        result = scrape_store_detail(store)
        result["map_url"] = build_map_url(result.pop("destination_id", None))

        directions_url = lookup_2gis(result["name"])
        result["directions_url"] = directions_url
        results.append(result)
        time.sleep(0.15)

        status = directions_url or "no 2GIS match"
        print(f"  [{i}/{len(stores)}] {result['name']} — {result.get('level')} | {status}")

    results.sort(key=lambda x: x.get("name", "").lower())

    with_2gis = sum(1 for r in results if r.get("directions_url"))
    print(f"\n{with_2gis}/{len(results)} stores have 2GIS directions URLs.")

    output_path = os.path.join(os.path.dirname(__file__), "mall_of_the_emirates_dine.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(results)} stores to {output_path}")


if __name__ == "__main__":
    main()
