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
        nextDataScript = soup.find("script", id="__NEXT_DATA__")
        return json.loads(nextDataScript.string)

    def _get_index_links(self):
        response = self._session.get(self.index_url, timeout=15)
        soup = BeautifulSoup(response.text, "html.parser")
        locationLinks = []
        for linkTag in soup.select("article section ol li em a[href]"):
            href = linkTag["href"]
            locationLinks.append(urljoin(BASE_URL, href))
        return locationLinks

    def _parse_location_page(self, url):
        pageData = self._get_next_data(url)
        document = pageData["props"]["pageProps"]["sliceDocument"]["data"]

        title = document.get("post_title", "")
        body = document.get("body", [])
        content = []

        for pageSlice in body:
            sliceType = pageSlice.get("slice_type")
            if sliceType == "image":
                imageData = pageSlice.get("primary", {}).get("image", {})
                imageUrl = imageData.get("url")
                if imageUrl:
                    content.append({"type": "image", "url": imageUrl})
            elif sliceType == "text_content":
                for block in pageSlice.get("primary", {}).get("content", []):
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
        locationLinks = self._get_index_links()
        results = [self._parse_location_page(url) for url in locationLinks]
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
