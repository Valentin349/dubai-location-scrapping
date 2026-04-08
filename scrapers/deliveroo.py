import json
import os
import sys
from curl_cffi import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from baseScraper import BaseScraper

BASE_URL = "https://rider.deliveroo.ae"
PARTNER_URL = f"{BASE_URL}/news/deliveroo-partner-locations"
RESIDENCE_URL = f"{BASE_URL}/news/deliveroo-residence-locations"


class _DeliverooBaseScraper(BaseScraper):
    """Shared HTTP logic for both Deliveroo scraper variants."""

    def __init__(self):
        self._session = requests.Session(impersonate="chrome120")

    @property
    def index_url(self):
        raise NotImplementedError

    def _get_next_data(self, url):
        response = self._session.get(url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        script = soup.find("script", id="__NEXT_DATA__")
        return json.loads(script.string)

    def _get_index_links(self):
        soup = BeautifulSoup(self._session.get(self.index_url, timeout=15).text, "html.parser")
        links = []
        for tag in soup.select("article section ol li em a[href]"):
            href = tag["href"]
            links.append(urljoin(BASE_URL, href))
        return links

    def _parse_location_page(self, url):
        data = self._get_next_data(url)
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
                for block in slice_.get("primary", {}).get("content", []):
                    text = block.get("text", "").strip()
                    if text:
                        content.append({"type": "instruction", "text": text})

        return {
            "name": title,
            "url": url,
            "phone": None,
            "floor": None,
            "unit": None,
            "content": content,
            "map_url": None,
            "directions_url": None,
        }

    def scrape(self):
        print(f"Fetching {self.index_url} ...")
        links = self._get_index_links()
        results = [self._parse_location_page(url) for url in links]
        print(f"Found {len(results)} locations.")
        return results


class DeliverooPartnerScraper(_DeliverooBaseScraper):

    @property
    def name(self):
        return "Deliveroo Partner Locations"

    @property
    def output_file(self):
        return os.path.join(os.path.dirname(os.path.dirname(__file__)), "results", "partner_results.json")

    @property
    def index_url(self):
        return PARTNER_URL


class DeliverooResidenceScraper(_DeliverooBaseScraper):

    @property
    def name(self):
        return "Deliveroo Residence Locations"

    @property
    def output_file(self):
        return os.path.join(os.path.dirname(os.path.dirname(__file__)), "results", "residence_results.json")

    @property
    def index_url(self):
        return RESIDENCE_URL


if __name__ == "__main__":
    DeliverooPartnerScraper().run()
    DeliverooResidenceScraper().run()
