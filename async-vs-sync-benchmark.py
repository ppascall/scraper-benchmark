#!/usr/bin/env python3
"""
ASYNC vs SYNC SCRAPING BENCHMARK
Compare performance of async vs synchronous scraping approaches
"""
import asyncio
import aiohttp
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import time
import random
import psutil
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import os
from tqdm import tqdm
import gc
import logging
from datetime import datetime

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'async-benchmark-{datetime.now().strftime("%Y%m%d-%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SystemMonitor:
    def __init__(self):
        self.monitoring = False
        self.cpu_samples = []
        self.memory_samples = []
        self.start_time = None
        
    def start_monitoring(self):
        self.monitoring = True
        self.start_time = time.time()
        self.cpu_samples = []
        self.memory_samples = []
        
        def monitor_loop():
            while self.monitoring:
                cpu = psutil.cpu_percent(interval=0.5)
                memory = psutil.virtual_memory()
                
                timestamp = time.time() - self.start_time
                
                self.cpu_samples.append({
                    'timestamp': timestamp,
                    'cpu_percent': cpu,
                })
                
                self.memory_samples.append({
                    'timestamp': timestamp,
                    'memory_used_gb': memory.used / (1024**3),
                    'memory_percent': memory.percent
                })
                
                time.sleep(0.5)
        
        self.monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        self.monitor_thread.start()
    
    def stop_monitoring(self):
        self.monitoring = False
        if hasattr(self, 'monitor_thread'):
            self.monitor_thread.join(timeout=2)
    
    def get_stats(self):
        if not self.cpu_samples:
            return {}
            
        cpu_values = [s['cpu_percent'] for s in self.cpu_samples]
        memory_values = [s['memory_used_gb'] for s in self.memory_samples]
        
        return {
            'cpu_avg': sum(cpu_values) / len(cpu_values),
            'cpu_max': max(cpu_values),
            'cpu_min': min(cpu_values),
            'memory_avg_gb': sum(memory_values) / len(memory_values),
            'memory_max_gb': max(memory_values),
            'memory_min_gb': min(memory_values),
            'sample_count': len(self.cpu_samples),
        }


class SyncScraper:
    """Traditional synchronous scraper with threading"""
    
    def __init__(self):
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        ]
    
    def create_session(self):
        """Create optimized session"""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            backoff_factor=0.5,
        )
        
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=20,
            pool_maxsize=20
        )
        
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        session.headers.update({
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        })
        
        return session
    
    def scrape_url(self, url, session):
        """Scrape single URL"""
        start_time = time.time()
        try:
            # Simulated scraping with realistic delay
            delay = random.uniform(0.01, 0.03)
            time.sleep(delay)
            
            # Simulate success/failure
            if random.random() > 0.90:
                return {
                    'url': url,
                    'status': 'failed',
                    'response_time': delay,
                }
            
            response_time = time.time() - start_time
            
            return {
                'url': url,
                'status': 'success',
                'response_time': response_time,
                'response_size': random.randint(15000, 45000),
            }
            
        except Exception as e:
            logger.error(f"Error: {e}")
            return {
                'url': url,
                'status': 'failed',
                'response_time': 0,
            }
    
    def worker_batch(self, urls, worker_id):
        """Worker function for batch processing"""
        session = self.create_session()
        results = []
        
        for url in urls:
            result = self.scrape_url(url, session)
            result['worker_id'] = worker_id
            results.append(result)
        
        return results
    
    def benchmark(self, urls, num_workers):
        """Run sync benchmark with threading"""
        logger.info(f"\n{'='*80}")
        logger.info(f"SYNC BENCHMARK (Threading)")
        logger.info(f"URLs: {len(urls)}, Workers: {num_workers}")
        logger.info(f"{'='*80}")
        
        monitor = SystemMonitor()
        monitor.start_monitoring()
        
        # Divide URLs among workers
        urls_per_worker = len(urls) // num_workers
        worker_tasks = []
        
        for i in range(num_workers):
            start_idx = i * urls_per_worker
            end_idx = start_idx + urls_per_worker
            if i == num_workers - 1:
                end_idx = len(urls)
            
            worker_urls = urls[start_idx:end_idx]
            worker_tasks.append((worker_urls, i))
        
        start_time = time.time()
        
        # Execute workers with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_to_worker = {
                executor.submit(self.worker_batch, task[0], task[1]): task[1]
                for task in worker_tasks
            }
            
            all_results = []
            
            for future in tqdm(as_completed(future_to_worker), total=len(worker_tasks), desc="Sync Workers"):
                try:
                    results = future.result()
                    all_results.extend(results)
                except Exception as e:
                    logger.error(f"Worker failed: {e}")
        
        total_time = time.time() - start_time
        monitor.stop_monitoring()
        
        return self.compile_results(all_results, total_time, monitor.get_stats(), "Sync (Threading)")


    def compile_results(self, results, total_time, system_stats, method_name):
        """Compile benchmark results"""
        successful = [r for r in results if r['status'] == 'success']
        failed = [r for r in results if r['status'] == 'failed']
        
        success_rate = (len(successful) / len(results)) * 100 if results else 0
        urls_per_second = len(successful) / total_time if total_time > 0 else 0
        
        response_times = [r['response_time'] for r in successful if 'response_time' in r]
        avg_response_time = sum(response_times) / len(response_times) if response_times else 0
        
        return {
            'method': method_name,
            'total_time': total_time,
            'total_urls': len(results),
            'successful': len(successful),
            'failed': len(failed),
            'success_rate': success_rate,
            'urls_per_second': urls_per_second,
            'avg_response_time': avg_response_time,
            'system_stats': system_stats
        }


class AsyncScraper:
    """Modern async scraper with aiohttp"""
    
    def __init__(self):
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        ]
    
    async def scrape_url(self, url, session, semaphore):
        """Scrape single URL asynchronously"""
        async with semaphore:
            start_time = time.time()
            try:
                # Simulated async scraping with realistic delay
                delay = random.uniform(0.01, 0.03)
                await asyncio.sleep(delay)
                
                # Simulate success/failure
                if random.random() > 0.90:
                    return {
                        'url': url,
                        'status': 'failed',
                        'response_time': delay,
                    }
                
                response_time = time.time() - start_time
                
                return {
                    'url': url,
                    'status': 'success',
                    'response_time': response_time,
                    'response_size': random.randint(15000, 45000),
                }
                
            except Exception as e:
                logger.error(f"Error: {e}")
                return {
                    'url': url,
                    'status': 'failed',
                    'response_time': 0,
                }
    
    async def scrape_batch(self, urls, concurrency):
        """Scrape batch of URLs asynchronously"""
        semaphore = asyncio.Semaphore(concurrency)
        
        timeout = aiohttp.ClientTimeout(total=30)
        connector = aiohttp.TCPConnector(limit=concurrency, limit_per_host=20)
        
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            tasks = [self.scrape_url(url, session, semaphore) for url in urls]
            
            results = []
            for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Async Tasks"):
                result = await coro
                results.append(result)
            
            return results
    
    def benchmark(self, urls, concurrency):
        """Run async benchmark"""
        logger.info(f"\n{'='*80}")
        logger.info(f"ASYNC BENCHMARK (asyncio + aiohttp)")
        logger.info(f"URLs: {len(urls)}, Concurrency: {concurrency}")
        logger.info(f"{'='*80}")
        
        monitor = SystemMonitor()
        monitor.start_monitoring()
        
        start_time = time.time()
        
        # Run async batch
        results = asyncio.run(self.scrape_batch(urls, concurrency))
        
        total_time = time.time() - start_time
        monitor.stop_monitoring()
        
        return self.compile_results(results, total_time, monitor.get_stats(), "Async (asyncio)")
    
    def compile_results(self, results, total_time, system_stats, method_name):
        """Compile benchmark results"""
        successful = [r for r in results if r['status'] == 'success']
        failed = [r for r in results if r['status'] == 'failed']
        
        success_rate = (len(successful) / len(results)) * 100 if results else 0
        urls_per_second = len(successful) / total_time if total_time > 0 else 0
        
        response_times = [r['response_time'] for r in successful if 'response_time' in r]
        avg_response_time = sum(response_times) / len(response_times) if response_times else 0
        
        return {
            'method': method_name,
            'total_time': total_time,
            'total_urls': len(results),
            'successful': len(successful),
            'failed': len(failed),
            'success_rate': success_rate,
            'urls_per_second': urls_per_second,
            'avg_response_time': avg_response_time,
            'system_stats': system_stats
        }


def print_comparison(sync_results, async_results):
    """Print comparative analysis"""
    logger.info(f"\n{'='*80}")
    logger.info("COMPARATIVE ANALYSIS: ASYNC vs SYNC")
    logger.info(f"{'='*80}")
    
    logger.info(f"\nPERFORMANCE COMPARISON:")
    logger.info(f"  Sync Time: {sync_results['total_time']:.2f}s")
    logger.info(f"  Async Time: {async_results['total_time']:.2f}s")
    
    speedup = sync_results['total_time'] / async_results['total_time'] if async_results['total_time'] > 0 else 0
    logger.info(f"  Speedup: {speedup:.2f}x {'faster' if speedup > 1 else 'slower'}")
    
    time_saved = sync_results['total_time'] - async_results['total_time']
    logger.info(f"  Time Saved: {time_saved:.2f}s ({time_saved/60:.2f} minutes)")
    
    logger.info(f"\nTHROUGHPUT COMPARISON:")
    logger.info(f"  Sync Rate: {sync_results['urls_per_second']:.2f} URLs/sec")
    logger.info(f"  Async Rate: {async_results['urls_per_second']:.2f} URLs/sec")
    
    throughput_improvement = (async_results['urls_per_second'] / sync_results['urls_per_second'] - 1) * 100 if sync_results['urls_per_second'] > 0 else 0
    logger.info(f"  Throughput Improvement: {throughput_improvement:.1f}%")
    
    logger.info(f"\nSUCCESS RATE COMPARISON:")
    logger.info(f"  Sync Success: {sync_results['success_rate']:.1f}%")
    logger.info(f"  Async Success: {async_results['success_rate']:.1f}%")
    
    logger.info(f"\nRESOURCE UTILIZATION:")
    logger.info(f"  Sync CPU Avg: {sync_results['system_stats'].get('cpu_avg', 0):.1f}%")
    logger.info(f"  Async CPU Avg: {async_results['system_stats'].get('cpu_avg', 0):.1f}%")
    logger.info(f"  Sync Memory Avg: {sync_results['system_stats'].get('memory_avg_gb', 0):.2f} GB")
    logger.info(f"  Async Memory Avg: {async_results['system_stats'].get('memory_avg_gb', 0):.2f} GB")
    
    logger.info(f"\nRECOMMENDATION:")
    if speedup > 3:
        logger.info(f"  STRONG RECOMMENDATION: Async provides {speedup:.1f}x improvement")
        logger.info(f"  Use async for production workloads")
    elif speedup > 1.5:
        logger.info(f"  MODERATE RECOMMENDATION: {speedup:.1f}x improvement")
        logger.info(f"  Async is better for most use cases")
    elif speedup > 1.1:
        logger.info(f"  SLIGHT IMPROVEMENT: {speedup:.1f}x faster")
        logger.info(f"  Async has marginal benefits")
    else:
        logger.info(f"  LIMITED BENEFIT: Similar performance")
        logger.info(f"  Choose based on code complexity preferences")
    
    logger.info(f"{'='*80}")


def load_or_generate_urls(count=1000):
    """Load URLs from cache or generate new ones"""
    try:
        with open('url_cache.json', 'r') as f:
            all_urls = json.load(f)
        urls = all_urls[:count]
        logger.info(f"Loaded {len(urls)} URLs from cache")
        return urls
    except FileNotFoundError:
        logger.info("url_cache.json not found. Please run comprehensive-benchmark.py first")
        # Generate simple test URLs as fallback
        urls = []
        for i in range(count):
            urls.append(f"http://books.toscrape.com/catalogue/page-{(i % 50) + 1}.html")
        logger.info(f"Generated {len(urls)} test URLs")
        return urls


def main():
    """Run async vs sync comparison benchmark"""
    logger.info("="*80)
    logger.info("ASYNC vs SYNC SCRAPING BENCHMARK")
    logger.info("="*80)
    
    # Test configurations
    num_urls = 1000
    sync_workers = 10
    async_concurrency = 50
    
    logger.info(f"\nConfiguration:")
    logger.info(f"  URLs to scrape: {num_urls}")
    logger.info(f"  Sync workers (threading): {sync_workers}")
    logger.info(f"  Async concurrency: {async_concurrency}")
    
    # Load URLs
    urls = load_or_generate_urls(num_urls)
    
    # Test 1: Sync scraping with threading
    logger.info("\n" + "="*80)
    logger.info("TEST 1: SYNCHRONOUS SCRAPING (Threading)")
    logger.info("="*80)
    
    sync_scraper = SyncScraper()
    sync_results = sync_scraper.benchmark(urls, sync_workers)
    
    logger.info(f"\nSync Results:")
    logger.info(f"  Time: {sync_results['total_time']:.2f}s")
    logger.info(f"  Success Rate: {sync_results['success_rate']:.1f}%")
    logger.info(f"  Throughput: {sync_results['urls_per_second']:.2f} URLs/sec")
    
    # Pause between tests
    logger.info("\nPausing 5 seconds between tests...")
    time.sleep(5)
    gc.collect()
    
    # Test 2: Async scraping with asyncio
    logger.info("\n" + "="*80)
    logger.info("TEST 2: ASYNCHRONOUS SCRAPING (asyncio)")
    logger.info("="*80)
    
    async_scraper = AsyncScraper()
    async_results = async_scraper.benchmark(urls, async_concurrency)
    
    logger.info(f"\nAsync Results:")
    logger.info(f"  Time: {async_results['total_time']:.2f}s")
    logger.info(f"  Success Rate: {async_results['success_rate']:.1f}%")
    logger.info(f"  Throughput: {async_results['urls_per_second']:.2f} URLs/sec")
    
    # Comparative analysis
    print_comparison(sync_results, async_results)
    
    # Save results
    comparison_results = {
        'timestamp': time.time(),
        'test_config': {
            'num_urls': num_urls,
            'sync_workers': sync_workers,
            'async_concurrency': async_concurrency
        },
        'sync_results': sync_results,
        'async_results': async_results,
        'speedup': sync_results['total_time'] / async_results['total_time'] if async_results['total_time'] > 0 else 0
    }
    
    filename = f"async-vs-sync-comparison-{int(time.time())}.json"
    with open(filename, 'w') as f:
        json.dump(comparison_results, f, indent=2)
    logger.info(f"\nResults saved to: {filename}")
    
    logger.info("\nBENCHMARK COMPLETE")
    logger.info("="*80)


if __name__ == "__main__":
    main()
