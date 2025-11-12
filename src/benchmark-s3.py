#!/usr/bin/env python3
import asyncio
import time
import psutil
from tqdm import tqdm
from functools import wraps
from urllib.parse import urljoin
from playwright.async_api import async_playwright
import requests
import requests_cache
import boto3
from dotenv import load_dotenv
import os
from bs4 import BeautifulSoup
import random

# -----------------------------
# LOAD ENV
# -----------------------------
load_dotenv()

BASE_URL = os.getenv("BASE_URL", "https://openlibrary.org")
SEARCH_QUERY = os.getenv("SEARCH_QUERY", "science_fiction")
CONCURRENCY_LIMIT = int(os.getenv("CONCURRENCY", 10))
BATCH_SIZE = 10

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET")

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

async def measure_resources_async(func, *args, **kwargs):
    process = psutil.Process()
    mem_before = process.memory_info().rss
    cpu_before = psutil.cpu_percent(interval=None)
    t0 = time.time()
    result = await func(*args, **kwargs)
    total_time = time.time() - t0
    mem_after = process.memory_info().rss
    cpu_after = psutil.cpu_percent(interval=None)
    mem_used = mem_after - mem_before
    cpu_used = cpu_after - cpu_before
    return result, total_time, mem_used, cpu_used

def requests_get_with_retries(session, url, retries=3, delay=0.5):
    for attempt in range(retries):
        try:
            return session.get(url, timeout=5)
        except:
            time.sleep(delay * (2 ** attempt))
    return None

async def retry_async(fn, retries=3, delay=0.5):
    for attempt in range(retries):
        try:
            return await fn()
        except:
            if attempt < retries - 1:
                await asyncio.sleep(delay * (2 ** attempt))
            else:
                return None

# -----------------------------
# REQUESTS + CACHING SCRAPER
# -----------------------------
@measure_resources
def requests_cached_scraper(max_pages=10, max_products=500):
    requests_cache.install_cache("scraper_cache", expire_after=3600)
    
    headers = {
        'User-Agent': f'Mozilla/5.0 (Linux; rv:91.0) Gecko/20100101 Firefox/91.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
    }
    session = requests.Session()
    session.headers.update(headers)
    
    all_links = []
    subjects = ['science_fiction', 'fantasy', 'mystery', 'romance', 'history', 'biography',
                'fiction', 'adventure', 'thriller', 'horror', 'drama', 'comedy', 'philosophy',
                'psychology', 'science']
    
    for page_num in tqdm(range(1, max_pages + 1), desc="Requests Cached Categories"):
        subject = subjects[page_num % len(subjects)]
        url = f"{BASE_URL}/search?subject={subject}"
        time.sleep(random.uniform(0.1, 0.3))  # Avoid rate limiting
        resp = requests_get_with_retries(session, url)
        if not resp:
            continue
        soup = BeautifulSoup(resp.text, "html.parser")
        books = soup.select("a[href*='/books/'], a[href*='/works/'], .book-cover a")
        links = [urljoin(BASE_URL, a["href"]) for a in books if a.get("href")]
        all_links.extend(links)
    
    all_links = all_links[:max_products]
    success_count = 0
    failed_count = 0
    latencies = []

    for link in tqdm(all_links, desc="Requests Cached Products"):
        t0 = time.time()
        resp = requests_get_with_retries(session, link)
        latency = time.time() - t0
        latencies.append(latency)
        if resp and resp.status_code == 200:
            success_count += 1
        else:
            failed_count += 1
        time.sleep(0.05)
    
    avg_latency = sum(latencies) / len(latencies) if latencies else 0
    return {
        "count": len(all_links),
        "success": success_count,
        "failed": failed_count,
        "avg_latency": avg_latency,
        "total_product_time": sum(latencies)
    }

# -----------------------------
# PLAYWRIGHT ASYNC CONCURRENT SCRAPER
# -----------------------------
async def playwright_scraper_async_concurrent(max_pages=10, max_products=100):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Linux; X11; Ubuntu; rv:91.0) Gecko/20100101 Firefox/91.0'
        )
        page = await context.new_page()
        
        all_links = []
        subjects = ['science_fiction', 'fantasy', 'mystery', 'romance', 'history', 'biography',
                    'fiction', 'adventure', 'thriller', 'horror', 'drama', 'comedy', 'philosophy',
                    'psychology', 'science']

        for page_num in tqdm(range(1, max_pages + 1), desc="Playwright Categories"):
            subject = subjects[page_num % len(subjects)]
            url = f"{BASE_URL}/search?subject={subject}"
            if page_num > 1:
                await asyncio.sleep(0.2)
            await page.goto(url, wait_until="load", timeout=30000)
            books = await page.query_selector_all("a[href*='/books/'], a[href*='/works/'], .book-cover a")
            links = []
            for a in books:
                href = await a.get_attribute("href")
                if href:
                    links.append(urljoin(BASE_URL, href))
            all_links.extend(links)
        
        all_links = all_links[:max_products]
        sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
        total_product_time = 0
        success_count = 0
        failed_count = 0
        latencies = []

        async def worker(link):
            async with sem:
                t0 = time.time()
                p = await context.new_page()
                try:
                    await retry_async(lambda: p.goto(link, wait_until="load", timeout=5000))
                    latency = time.time() - t0
                    return True, latency
                except:
                    latency = time.time() - t0
                    return False, latency
                finally:
                    await p.close()

        for i in tqdm(range(0, len(all_links), BATCH_SIZE), desc="Playwright Product Batches"):
            batch_links = all_links[i:i+BATCH_SIZE]
            results = await asyncio.gather(*[worker(link) for link in batch_links])
            for success, latency in results:
                latencies.append(latency)
                if success:
                    success_count += 1
                else:
                    failed_count += 1
            total_product_time += sum([lat for _, lat in results])
        
        await browser.close()
        avg_latency = sum(latencies) / len(latencies) if latencies else 0
        return {
            "count": len(all_links),
            "success": success_count,
            "failed": failed_count,
            "avg_latency": avg_latency,
            "total_product_time": total_product_time
        }

# -----------------------------
# UPLOAD RESULTS TO S3
# -----------------------------
def upload_results_s3(results, filename="benchmark_results.txt"):
    if not S3_BUCKET:
        print("No S3 bucket set, skipping upload.")
        return
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    with open(filename, "w") as f:
        for method, data in results.items():
            f.write(f"{method}: {data}\n")
    s3.upload_file(filename, S3_BUCKET, filename)
    print(f"\nResults uploaded to S3://{S3_BUCKET}/{filename}")

# -----------------------------
# RUN BENCHMARKS
# -----------------------------
def run_benchmarks():
    results = {}

    print("\n--- Requests Cached Benchmark ---")
    data = requests_cached_scraper()
    results["Requests Cached"] = data

    print("\n--- Playwright Async Concurrent Benchmark ---")
    data = asyncio.run(measure_resources_async(playwright_scraper_async_concurrent))[0]
    results["Playwright Async Concurrent"] = data

    print("\n================= BENCHMARK RESULTS =================")
    print(f"{'Method':30} {'Total':>8} {'Success':>8} {'Failed':>8} {'Avg Latency(s)':>15} {'Prod Time(s)':>15}")
    print("-" * 100)
    for method, data in results.items():
        print(f"{method:30} {data['count']:8} {data['success']:8} {data['failed']:8} {data['avg_latency']:15.2f} {data['total_product_time']:15.2f}")

    upload_results_s3(results)

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    run_benchmarks()
