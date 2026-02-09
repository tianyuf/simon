"""Scraper for Herbert Simon papers from CMU Digital Collections."""

import re
import time
import urllib3
from typing import Optional
from bs4 import BeautifulSoup
import requests
from tqdm import tqdm

# Disable SSL warnings since CMU has certificate issues
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://digitalcollections.library.cmu.edu"
SEARCH_URL = f"{BASE_URL}/search"

# Build search URL directly to avoid encoding issues
# Note: CMU only supports items_per_page values of 10 or 25
def build_search_url(page: int = 0, items_per_page: int = 25) -> str:
    """Build the search URL with proper encoding."""
    # Only 10 and 25 work; force valid values
    if items_per_page not in (10, 25):
        items_per_page = 25
    base = f"{SEARCH_URL}?search_api_fulltext=&title=&name=&cmu_date_ft=&cmu_subject="
    base += f"&sort_by=search_api_relevance&sort_order=DESC&items_per_page={items_per_page}"
    # Note: %20 encoding for space is required (not + sign)
    base += "&search_advanced%5B0%5D=cmu_collection%3AHerbert%20Simon"
    if page > 0:
        base += f"&page={page}"
    return base


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}


def fetch_page(page: int, items_per_page: int = 25, max_retries: int = 3) -> Optional[str]:
    """Fetch a single page of search results."""
    url = build_search_url(page, items_per_page)

    for attempt in range(max_retries):
        try:
            response = requests.get(
                url,
                headers=HEADERS,
                verify=False,  # CMU has cert issues
                timeout=30
            )
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                print(f"Failed to fetch page {page}: {e}")
                return None

    return None


def parse_search_results(html: str) -> list[dict]:
    """Parse search results HTML and extract paper metadata."""
    soup = BeautifulSoup(html, 'lxml')
    papers = []

    # Find all search result rows (direct children only to avoid nested rows)
    for row in soup.select('.view-content > .views-row'):
        paper = {}

        # Extract title and URL
        title_link = row.select_one('.search-details h2 a')
        if title_link:
            paper['title'] = title_link.get_text(strip=True)
            href = title_link.get('href', '')
            # Clean up the URL (remove query params)
            href = href.split('?')[0]
            paper['url'] = f"{BASE_URL}{href}"

            # Extract node_id from URL
            node_match = re.search(r'/node/(\d+)', href)
            if node_match:
                paper['node_id'] = int(node_match.group(1))

        # Extract date and series from strong tags
        for strong in row.select('.search-details strong'):
            label = strong.get_text(strip=True).rstrip(':')
            value = strong.next_sibling
            if value:
                value = value.strip() if isinstance(value, str) else value.get_text(strip=True)

            if label == 'Date' and value:
                paper['date'] = value
                # Try to extract sortable date (YYYY-MM-DD format from raw HTML)
                raw_html = str(row)
                iso_match = re.search(r'(\d{4}-\d{2}-\d{2})', raw_html)
                if iso_match:
                    paper['date_sort'] = iso_match.group(1)
                else:
                    # Try to parse from display date
                    year_match = re.search(r'\b(1\d{3}|20\d{2})\b', value)
                    if year_match:
                        paper['date_sort'] = year_match.group(1)
            elif label == 'Series' and value:
                paper['series'] = value

        # Extract item type from title (often in format "Type -- Title")
        if 'title' in paper:
            title = paper['title']
            # Common patterns: "Reprint #XXX", "Book", "Memo", etc.
            type_patterns = [
                (r'^(Reprint #\d+)', 'article'),
                (r'^(Book Chapter)', 'chapter'),
                (r'^(Book Review)', 'review'),
                (r'^(Book)\s+--', 'book'),
                (r'^(Memo)\s+--', 'memorandum'),
                (r'^(Letter)', 'correspondence'),
            ]
            for pattern, item_type in type_patterns:
                if re.match(pattern, title, re.IGNORECASE):
                    paper['item_type'] = item_type
                    break

        # Extract thumbnail and box/folder info
        thumb_img = row.select_one('.search-image img')
        if thumb_img:
            thumb_src = thumb_img.get('src', '')
            if thumb_src:
                paper['thumbnail_url'] = f"{BASE_URL}{thumb_src}" if thumb_src.startswith('/') else thumb_src

                # Extract box/folder/bundle/document from thumbnail filename
                # Format: Simon_box00069_fld05305_bdl0001_doc0001.jpg
                archive_match = re.search(
                    r'Simon_box(\d+)_fld(\d+)_bdl(\d+)_doc(\d+)',
                    thumb_src
                )
                if archive_match:
                    paper['box_number'] = int(archive_match.group(1))
                    paper['folder_number'] = int(archive_match.group(2))
                    paper['bundle_number'] = int(archive_match.group(3))
                    paper['document_number'] = int(archive_match.group(4))

        if paper.get('node_id'):
            papers.append(paper)

    return papers


def get_total_count(html: str) -> int:
    """Extract total result count from search page."""
    soup = BeautifulSoup(html, 'lxml')

    # Look for the collection facet count
    facet = soup.select_one('[data-drupal-facet-item-value="Herbert Simon"] .facet-item__count')
    if facet:
        count_text = facet.get_text(strip=True)
        count_match = re.search(r'\((\d+)\)', count_text)
        if count_match:
            return int(count_match.group(1))

    return 0


def scrape_all(items_per_page: int = 25, delay: float = 0.5, start_page: int = 0) -> list[dict]:
    """
    Scrape all Herbert Simon papers from CMU Digital Collections.

    Args:
        items_per_page: Number of items per page (max 50)
        delay: Delay between requests in seconds
        start_page: Page to start from (for resuming)

    Returns:
        List of paper dictionaries
    """
    all_papers = []

    # Fetch first page to get total count
    print("Fetching first page to determine total count...")
    first_html = fetch_page(0, items_per_page)
    if not first_html:
        print("Failed to fetch first page")
        return []

    total_count = get_total_count(first_html)
    if total_count == 0:
        print("Could not determine total count")
        return []

    total_pages = (total_count + items_per_page - 1) // items_per_page
    print(f"Found {total_count} items across {total_pages} pages")

    # Parse first page if not resuming
    if start_page == 0:
        papers = parse_search_results(first_html)
        all_papers.extend(papers)
        start_page = 1

    # Fetch remaining pages
    for page in tqdm(range(start_page, total_pages), desc="Scraping pages"):
        time.sleep(delay)

        html = fetch_page(page, items_per_page)
        if html:
            papers = parse_search_results(html)
            all_papers.extend(papers)

            # Progress checkpoint every 100 pages
            if page % 100 == 0:
                print(f"\nCheckpoint: {len(all_papers)} papers scraped so far")

    print(f"\nScraped {len(all_papers)} papers total")
    return all_papers


def scrape_and_save(items_per_page: int = 25, delay: float = 0.5):
    """Scrape all papers and save to database incrementally."""
    import sys
    sys.path.insert(0, str(__file__).rsplit('/', 2)[0])
    from db import init_db, insert_papers_batch

    # Initialize database
    init_db()

    # Fetch first page to get total count
    print("Fetching first page to determine total count...")
    first_html = fetch_page(0, items_per_page)
    if not first_html:
        print("Failed to fetch first page")
        return

    total_count = get_total_count(first_html)
    total_pages = (total_count + items_per_page - 1) // items_per_page
    print(f"Found {total_count} items across {total_pages} pages")

    # Process first page
    papers = parse_search_results(first_html)
    inserted = insert_papers_batch(papers)
    total_inserted = inserted

    # Fetch remaining pages
    for page in tqdm(range(1, total_pages), desc="Scraping"):
        time.sleep(delay)

        html = fetch_page(page, items_per_page)
        if html:
            papers = parse_search_results(html)
            inserted = insert_papers_batch(papers)
            total_inserted += inserted

            # Progress checkpoint every 50 pages
            if page % 50 == 0:
                tqdm.write(f"Checkpoint: {total_inserted} new papers inserted")

    print(f"\nDone! Inserted {total_inserted} new papers")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape Herbert Simon papers from CMU")
    parser.add_argument("--items-per-page", type=int, default=50, help="Items per page")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between requests")
    parser.add_argument("--test", action="store_true", help="Test mode: only fetch first page")

    args = parser.parse_args()

    if args.test:
        print("Test mode: fetching first page only")
        html = fetch_page(0, args.items_per_page)
        if html:
            papers = parse_search_results(html)
            print(f"Found {len(papers)} papers on first page")
            for p in papers[:3]:
                print(f"  - {p.get('title', 'N/A')[:60]}...")
                print(f"    Date: {p.get('date', 'N/A')}, Series: {p.get('series', 'N/A')}")
    else:
        scrape_and_save(args.items_per_page, args.delay)
