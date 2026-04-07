import json
import os
import time
import requests


BASE_URL = "https://thedubaimall.com"
STORES_URL = f"{BASE_URL}/en/data/get-all-stores"

ABUZZ_EMBED = "https://map.abuzz.tech/amap/embedex.php"
ABUZZ_API_KEY = "AC6B0D339940194AB7E2BC8E1967A4AE"
ABUZZ_SITE = "TDM_"

TWOGIS_CATALOG_API = "https://catalog.api.2gis.com/3.0/items"
TWOGIS_KEY = "ruregt3044"

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


def parse_unit_number(unit_number):
    if not unit_number:
        return None
    parts = unit_number.split("-")
    if len(parts) < 3:
        return unit_number
    zone_code = parts[1]
    unit_suffix = "-".join(parts[2:])
    zone_name = ZONE_MAP.get(zone_code, zone_code)
    return f"{zone_name}, Unit {unit_suffix}"


def build_map_url(unit_number):
    if not unit_number:
        return None
    return (
        f"{ABUZZ_EMBED}?site={ABUZZ_SITE}&apiKey={ABUZZ_API_KEY}"
        f"&searchUI=true&servicesList=true&baseui=true"
        f"&poiUI=true&pathUI=true&hover=true&lazyld=false&mobile=true"
        f"&node={unit_number}"
    )


def lookup_2gis(name, lat, lng):
    """Search 2GIS by name anchored to the store's closest parking entrance coords."""
    try:
        r = requests.get(TWOGIS_CATALOG_API, params={
            "q": name,
            "point": f"{lng},{lat}",
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
        item_lat = point.get("lat")
        item_lon = point.get("lon")
        full_id = item.get("id", "")
        short_id = full_id.split("_")[0] if full_id else None
        if item_lat and item_lon and short_id:
            return f"https://2gis.ae/dubai/directions/points/%7C{item_lon}%2C{item_lat}%3B{short_id}"
    except Exception:
        pass
    return None


def fetch_all_stores():
    response = requests.get(STORES_URL, timeout=15)
    response.raise_for_status()
    return response.json()


def main():
    print("Fetching all stores from The Dubai Mall...")
    stores = fetch_all_stores()
    print(f"Total stores fetched: {len(stores)}")

    dine_stores = [
        s for s in stores
        if any(c.get("id") in DINE_CATEGORY_IDS for c in (s.get("categories") or []))
    ]
    print(f"Dine stores: {len(dine_stores)} — looking up 2GIS URLs...")

    results = []
    for i, store in enumerate(dine_stores, 1):
        name = store.get("name")
        unit_number = store.get("unitNumber")

        # Use closest parking entrance coords as 2GIS search anchor
        parking = store.get("closestParkingEntrance") or {}
        lat = parking.get("latitude")
        lng = parking.get("longitude")

        directions_url = lookup_2gis(name, lat, lng) if lat and lng else None
        time.sleep(0.15)

        status = directions_url or "no 2GIS match"
        print(f"  [{i}/{len(dine_stores)}] {name} — {unit_number} | {status}")

        results.append({
            "name": name,
            "url": f"{BASE_URL}/en/shop/{store.get('slug')}",
            "phone": store.get("phone"),
            "unit_number": unit_number,
            "location": parse_unit_number(unit_number),
            "map_url": build_map_url(unit_number),
            "directions_url": directions_url,
        })

    with_2gis = sum(1 for r in results if r.get("directions_url"))
    print(f"\n{with_2gis}/{len(results)} stores have 2GIS directions URLs.")

    output_path = os.path.join(os.path.dirname(__file__), "dubai_mall_dine.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(results)} stores to {output_path}")


if __name__ == "__main__":
    main()
