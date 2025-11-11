#!/usr/bin/env python3
import time
import psutil
from tqdm import tqdm
from functools import wraps
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright

# -----------------------------
# CONFIG
# -----------------------------
BASE_URL = "https://webscraper.io"
TEST_SITES = {
    "Static": "/test-sites/e-commerce/allinone",
    "Pagination": "/test-sites/e-commerce/static",
    "AJAX Pagination": "/test-sites/e-commerce/ajax",
    "Load More": "/test-sites/e-commerce/more",
    "Scrolling": "/test-sites/e-commerce/scroll",
}

# -----------------------------
# UTILS
# -----------------------------
def measure_resources(func):
    """Decorator to measure time, memory, and CPU usage."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        process = psutil.Process()
        mem_before = process.memory_info().rss
        cpu_before = psutil.cpu_percent(interval=None)
        t0 = time.time()

        result = func(*args, **kwargs)

        total_time = time.time() - t0
        mem_after = process.memory_info().rss
        cpu_after = psutil.cpu_percent(interval=None)
        mem_used = mem_after - mem_before
        cpu_used = cpu_after - cpu_before
        return result, total_time, mem_used, cpu_used
    return wrapper

# -----------------------------
# PLAYWRIGHT SCRAPER
# -----------------------------
@measure_resources
def playwright_scraper(site_name, path, headless=True):
    """Scrape product data using Playwright on a given site."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()

        url = urljoin(BASE_URL, path)
        print(f"\n--- {site_name} ({'Headless' if headless else 'GUI'}) ---")
        print(f"Loading: {url}")

        # Load main page
        main_t0 = time.time()
        page.goto(url, wait_until="load")
        main_time = time.time() - main_t0

        # Handle special dynamic types
        if "more" in path:
            # Click "Load more" until no button is found
            while True:
                button = page.query_selector("button.load-more")
                if not button:
                    break
                button.click()
                page.wait_for_timeout(1000)
        elif "scroll" in path:
            # Scroll down until all products are loaded
            prev_height = 0
            while True:
                page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
                page.wait_for_timeout(1000)
                new_height = page.evaluate("document.body.scrollHeight")
                if new_height == prev_height:
                    break
                prev_height = new_height

        # Collect product links
        products = page.query_selector_all(".thumbnail a.title")
        product_links = [urljoin(BASE_URL, p.get_attribute("href")) for p in products]

        # Visit each product page
        product_times = []
        for link in tqdm(product_links, desc=f"{site_name} Products"):
            t0 = time.time()
            page.goto(link, wait_until="load")
            product_times.append(time.time() - t0)

        browser.close()
        return len(product_links), main_time, sum(product_times)

# -----------------------------
# RUN BENCHMARKS
# -----------------------------
def run_benchmarks():
    results = {}

    for site_name, path in TEST_SITES.items():
        (count, main_time, prod_time), total_time, mem, cpu = playwright_scraper(site_name, path, headless=True)
        results[site_name] = {
            "products": count,
            "main_page_time": main_time,
            "product_pages_time": prod_time,
            "total_runtime": total_time,
            "memory_used_MB": mem / (1024 ** 2),
            "cpu_delta": cpu,
        }

    print("\n================= RESULTS =================")
    for name, data in results.items():
        print(f"{name}:")
        print(f"  Products scraped:     {data['products']}")
        print(f"  Main page time:       {data['main_page_time']:.2f}s")
        print(f"  Product pages time:   {data['product_pages_time']:.2f}s")
        print(f"  Total runtime:        {data['total_runtime']:.2f}s")
        print(f"  Memory used:          {data['memory_used_MB']:.2f} MB")
        print(f"  CPU percent delta:    {data['cpu_delta']:.2f}%")
        print("")

if __name__ == "__main__":
    run_benchmarks()
