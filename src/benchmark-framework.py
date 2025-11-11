#!/usr/bin/env python3
import time
import psutil
import asyncio
from tqdm import tqdm
from functools import wraps
from urllib.parse import urljoin

# -----------------------------
# CONFIG
# -----------------------------
BASE_URL = "https://webscraper.io"
MAIN_PAGE = "/test-sites/e-commerce/allinone/computers/laptops"

# -----------------------------
# UTILS
# -----------------------------
def measure_resources(func):
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
# BEAUTIFULSOUP SCRAPER
# -----------------------------
@measure_resources
def bs_scraper():
    import requests
    from bs4 import BeautifulSoup

    main_t0 = time.time()
    r = requests.get(urljoin(BASE_URL, MAIN_PAGE))
    soup = BeautifulSoup(r.text, "html.parser")
    main_time = time.time() - main_t0

    product_links = [
        urljoin(BASE_URL, a["href"]) for a in soup.select(".thumbnail a.title")
    ]

    product_times = []
    for link in tqdm(product_links, desc="BS Products"):
        t0 = time.time()
        r = requests.get(link)
        _ = BeautifulSoup(r.text, "html.parser")  # parse product page
        product_times.append(time.time() - t0)

    return len(product_links), sum(product_times)

# -----------------------------
# SELENIUM SCRAPER
# -----------------------------
@measure_resources
def selenium_scraper(headless=True):
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By

    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")

    driver = webdriver.Chrome(options=options)

    driver.get(urljoin(BASE_URL, MAIN_PAGE))
    products = driver.find_elements(By.CSS_SELECTOR, ".thumbnail a.title")
    product_links = [urljoin(BASE_URL, p.get_attribute("href")) for p in products]

    product_times = []
    for link in tqdm(product_links, desc=f"Selenium {'Headless' if headless else 'GUI'} Products"):
        t0 = time.time()
        driver.get(link)
        product_times.append(time.time() - t0)

    driver.quit()
    return len(product_links), sum(product_times)

# -----------------------------
# PLAYWRIGHT SCRAPER
# -----------------------------
@measure_resources
def playwright_scraper(headless=True):
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()
        page.goto(urljoin(BASE_URL, MAIN_PAGE))

        products = page.query_selector_all(".thumbnail a.title")
        product_links = [urljoin(BASE_URL, p.get_attribute("href")) for p in products]

        product_times = []
        for link in tqdm(product_links, desc=f"Playwright {'Headless' if headless else 'GUI'} Products"):
            t0 = time.time()
            page.goto(link)
            product_times.append(time.time() - t0)

        browser.close()
        return len(product_links), sum(product_times)

# -----------------------------
# RUN BENCHMARKS
# -----------------------------
def run_benchmarks():
    tests = {
        "BeautifulSoup": bs_scraper,
        "Selenium Headless": lambda: selenium_scraper(headless=True),
        "Selenium GUI": lambda: selenium_scraper(headless=False),
        "Playwright Headless": lambda: playwright_scraper(headless=True),
        "Playwright GUI": lambda: playwright_scraper(headless=False),
    }

    results = {}
    for name, func in tests.items():
        print(f"\nRunning {name}...")
        (count, prod_time), main_time, mem, cpu = func()
        results[name] = {
            "products": count,
            "main_page_time": main_time,
            "product_pages_time": prod_time,
            "memory": mem,
            "cpu": cpu,
        }

    print("\n--- RESULTS ---")
    for name, data in results.items():
        print(f"{name}:")
        print(f"  Products scraped: {data['products']}")
        print(f"  Main page time: {data['main_page_time']:.2f}s")
        print(f"  Product pages time: {data['product_pages_time']:.2f}s")
        print(f"  Memory used: {data['memory'] / (1024**2):.2f} MB")
        print(f"  CPU percent delta: {data['cpu']:.2f}%")
        print("")

if __name__ == "__main__":
    run_benchmarks()
