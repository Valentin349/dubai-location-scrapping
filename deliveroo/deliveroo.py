import json
import os
from curl_cffi import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE_URL = "https://rider.deliveroo.ae"
PARTNER_URL = f"{BASE_URL}/news/deliveroo-partner-locations"
RESIDENCE_URL = f"{BASE_URL}/news/deliveroo-residence-locations"


def make_scraper():
    return requests.Session(impersonate="chrome120")


def get_next_data(scraper, url):
    response = scraper.get(url, timeout=15)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    script = soup.find("script", id="__NEXT_DATA__")
    return json.loads(script.string)


def get_index_links(scraper, index_url):
    """Return all location URLs from an index page."""
    soup = BeautifulSoup(scraper.get(index_url, timeout=15).text, "html.parser")
    links = []
    for tag in soup.select("article section ol li em a[href]"):
        href = tag["href"]
        full_url = urljoin(BASE_URL, href)
        links.append(full_url)
    return links


def parse_location_page(scraper, url):
    """Extract title and interleaved instructions/images from a location page."""
    data = get_next_data(scraper, url)
    doc = data["props"]["pageProps"]["sliceDocument"]["data"]

    title = doc.get("post_title", "")
    body = doc.get("body", [])

    content = []

    for slice_ in body:
        slice_type = slice_.get("slice_type")

        if slice_type == "image":
            img = slice_.get("primary", {}).get("image", {})
            img_url = img.get("url")
            if img_url:
                content.append({"type": "image", "url": img_url})

        elif slice_type == "text_content":
            content_blocks = slice_.get("primary", {}).get("content", [])
            for block in content_blocks:
                text = block.get("text", "").strip()
                if text:
                    content.append({"type": "instruction", "text": text})

    return {"title": title, "url": url, "content": content}


def scrape_index(scraper, index_url, output_filename):
    print(f"Fetching {index_url} ...")
    links = get_index_links(scraper, index_url)
    print(f"Found {len(links)} location links\n")

    results = []
    for url in links:
        print(f"  Scraping: {url}")
        results.append(parse_location_page(scraper, url))

    output_path = os.path.join(os.path.dirname(__file__), output_filename)
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(results)} locations to {output_path}\n")


def main():
    scraper = make_scraper()
    scrape_index(scraper, PARTNER_URL, "partner_results.json")
    scrape_index(scraper, RESIDENCE_URL, "residence_results.json")


if __name__ == "__main__":
    main()
