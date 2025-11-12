#!/usr/bin/env python3
"""
PRODUCTION PREMIUM PROXY SCRAPER
Ready for real proxy services (SmartProxy, BrightData, etc.)
"""
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import time
import random
import itertools
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import os
import boto3
from tqdm import tqdm

load_dotenv()

class ProductionProxyScraper:
    def __init__(self):
        self.s3_bucket = os.getenv("S3_BUCKET", "my-scraper-results-2025")
        self.aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        
        # Premium proxy configurations
        self.proxy_configs = {
            'smartproxy': {
                'endpoint': 'gate.smartproxy.com:7000',
                'username': os.getenv('SMARTPROXY_USERNAME', 'user'),
                'password': os.getenv('SMARTPROXY_PASSWORD', 'pass'),
                'type': 'residential',
                'rotation': 'sticky_session'  # or 'per_request'
            },
            'brightdata': {
                'endpoint': 'brd.superproxy.io:22225',
                'username': os.getenv('BRIGHTDATA_USERNAME', 'user'),
                'password': os.getenv('BRIGHTDATA_PASSWORD', 'pass'),
                'type': 'residential',
                'rotation': 'per_request'
            },
            'oxylabs': {
                'endpoint': 'pr.oxylabs.io:7777',
                'username': os.getenv('OXYLABS_USERNAME', 'user'),
                'password': os.getenv('OXYLABS_PASSWORD', 'pass'),
                'type': 'datacenter',
                'rotation': 'sticky_session'
            }
        }
        
        # For demo purposes - simulate premium proxy performance
        self.demo_mode = True
        self.current_proxy_service = 'smartproxy'  # Default
        
        # Advanced user agent rotation
        self.enterprise_user_agents = [
            # Chrome - Latest versions across platforms
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            
            # Firefox - Multiple versions
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:119.0) Gecko/20100101 Firefox/119.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:120.0) Gecko/20100101 Firefox/120.0',
            'Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0',
            
            # Safari - macOS versions
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
            
            # Edge - Windows versions
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0'
        ]
        
        # Geographic regions for residential proxies
        self.geo_regions = [
            {'country': 'US', 'lang': 'en-US,en;q=0.9', 'timezone': 'America/New_York'},
            {'country': 'GB', 'lang': 'en-GB,en;q=0.8', 'timezone': 'Europe/London'},
            {'country': 'CA', 'lang': 'en-CA,en;q=0.7,fr;q=0.3', 'timezone': 'America/Toronto'},
            {'country': 'AU', 'lang': 'en-AU,en;q=0.9', 'timezone': 'Australia/Sydney'},
            {'country': 'DE', 'lang': 'en-US,en;q=0.7,de;q=0.3', 'timezone': 'Europe/Berlin'},
            {'country': 'FR', 'lang': 'en-US,en;q=0.6,fr;q=0.4', 'timezone': 'Europe/Paris'},
            {'country': 'NL', 'lang': 'en-US,en;q=0.8,nl;q=0.2', 'timezone': 'Europe/Amsterdam'},
            {'country': 'JP', 'lang': 'en-US,en;q=0.9,ja;q=0.1', 'timezone': 'Asia/Tokyo'}
        ]
        
    def create_premium_session(self, proxy_service='smartproxy', session_id=None):
        """Create session with premium proxy configuration"""
        session = requests.Session()
        
        # Configure retry strategy for production
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504, 520, 521, 522, 524],
            allowed_methods=["HEAD", "GET", "OPTIONS"],  # Updated API
            backoff_factor=2,
            raise_on_redirect=False,
            raise_on_status=False
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=100, pool_maxsize=100)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set up premium proxy
        if not self.demo_mode and proxy_service in self.proxy_configs:
            proxy_config = self.proxy_configs[proxy_service]
            
            # Residential proxy with authentication
            proxy_url = f"http://{proxy_config['username']}:{proxy_config['password']}@{proxy_config['endpoint']}"
            
            # Add session ID for sticky sessions
            if session_id and proxy_config.get('rotation') == 'sticky_session':
                proxy_url = f"http://{proxy_config['username']}-session-{session_id}:{proxy_config['password']}@{proxy_config['endpoint']}"
            
            session.proxies = {
                'http': proxy_url,
                'https': proxy_url
            }
        
        # Advanced header configuration
        geo_region = random.choice(self.geo_regions)
        user_agent = random.choice(self.enterprise_user_agents)
        
        # Comprehensive browser headers
        headers = {
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': geo_region['lang'],
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
            'DNT': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1'
        }
        
        # Browser-specific headers
        if 'Chrome' in user_agent:
            headers.update({
                'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': f'"{random.choice(["Windows", "macOS", "Linux"])}"'
            })
        
        # Add referer sometimes
        if random.random() > 0.3:
            headers['Referer'] = random.choice([
                'https://www.google.com/',
                'https://www.bing.com/',
                'https://duckduckgo.com/'
            ])
        
        session.headers.update(headers)
        
        # Session cookies for persistence
        session.cookies.set('session_type', 'premium_proxy')
        session.cookies.set('region', geo_region['country'])
        
        return session
    
    def simulate_premium_performance(self, base_success_rate=0.9, base_speed_multiplier=15):
        """Simulate premium proxy performance characteristics"""
        # Premium proxies have high success rates but some variation
        success_probability = base_success_rate + random.uniform(-0.05, 0.05)
        
        # Speed varies by proxy quality and region
        speed_multiplier = base_speed_multiplier + random.uniform(-3, 5)
        
        # Simulate occasional proxy rotation delays
        rotation_delay = 0
        if random.random() > 0.85:  # 15% chance of rotation delay
            rotation_delay = random.uniform(0.5, 2.0)
        
        return success_probability, speed_multiplier, rotation_delay
    
    def premium_scrape_batch(self, urls, worker_id=0, proxy_service='smartproxy'):
        """Scrape batch with premium proxy service"""
        print(f"🔥 Premium Worker {worker_id} starting {len(urls)} URLs with {proxy_service}")
        
        # Create premium session
        session = self.create_premium_session(proxy_service, session_id=f"worker_{worker_id}")
        
        successful = 0
        failed = 0
        start_time = time.time()
        details = []
        
        for i, url in enumerate(urls):
            try:
                # Premium proxy characteristics
                if self.demo_mode:
                    success_prob, speed_mult, rotation_delay = self.simulate_premium_performance()
                    
                    # Simulate faster processing with premium infrastructure
                    processing_delay = random.uniform(0.1, 0.3)  # Much faster than free
                    time.sleep(processing_delay + rotation_delay)
                    
                    # Simulate high success rate
                    if random.random() < success_prob:
                        # Simulate successful response
                        successful += 1
                        details.append({
                            'url': url,
                            'status': 'success',
                            'proxy_service': proxy_service,
                            'simulated': True,
                            'worker_id': worker_id
                        })
                    else:
                        failed += 1
                        details.append({
                            'url': url,
                            'status': 'failed',
                            'reason': 'simulated_rate_limit',
                            'worker_id': worker_id
                        })
                        
                else:
                    # Real premium proxy request
                    response = session.get(url, timeout=15)
                    response.raise_for_status()
                    
                    if len(response.text) > 500:
                        successful += 1
                        details.append({
                            'url': url,
                            'status': 'success',
                            'proxy_service': proxy_service,
                            'response_size': len(response.text),
                            'worker_id': worker_id
                        })
                    else:
                        failed += 1
                
                # Progress reporting
                if (i + 1) % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = (i + 1) / elapsed
                    print(f"    Worker {worker_id}: {i+1}/{len(urls)} ({rate:.1f}/sec, {successful}/{i+1} success)")
                
            except Exception as e:
                failed += 1
                details.append({
                    'url': url,
                    'status': 'failed',
                    'reason': str(e),
                    'worker_id': worker_id
                })
        
        total_time = time.time() - start_time
        worker_rate = successful / total_time if total_time > 0 else 0
        
        result = {
            'worker_id': worker_id,
            'proxy_service': proxy_service,
            'urls_processed': len(urls),
            'successful': successful,
            'failed': failed,
            'processing_time': total_time,
            'rate': worker_rate,
            'success_percentage': (successful / len(urls)) * 100,
            'details': details
        }
        
        print(f"✅ Worker {worker_id} completed: {successful}/{len(urls)} URLs ({worker_rate:.1f}/sec, {result['success_percentage']:.1f}% success)")
        
        # Upload to S3
        try:
            s3 = boto3.client('s3', region_name=self.aws_region)
            s3.put_object(
                Bucket=self.s3_bucket,
                Key=f'premium-results/worker-{worker_id}-{proxy_service}.json',
                Body=json.dumps(result, indent=2)
            )
            print(f"📊 Worker {worker_id} results uploaded to S3")
        except Exception as e:
            print(f"S3 upload error: {e}")
        
        return result
    
    def run_premium_benchmark(self, urls, num_workers=10, proxy_service='smartproxy'):
        """Run production-grade premium proxy benchmark"""
        print(f"🚀 PREMIUM PROXY BENCHMARK")
        print(f"=" * 50)
        print(f"Service: {proxy_service.title()}")
        print(f"URLs: {len(urls)}")
        print(f"Workers: {num_workers}")
        print(f"Mode: {'Demo Simulation' if self.demo_mode else 'Real Proxies'}")
        print(f"=" * 50)
        
        # Divide URLs among workers
        urls_per_worker = len(urls) // num_workers
        worker_tasks = []
        
        for i in range(num_workers):
            start_idx = i * urls_per_worker
            end_idx = start_idx + urls_per_worker
            if i == num_workers - 1:  # Last worker gets remainder
                end_idx = len(urls)
            
            worker_urls = urls[start_idx:end_idx]
            worker_tasks.append((worker_urls, i, proxy_service))
            print(f"  🔧 Worker {i}: {len(worker_urls)} URLs")
        
        print(f"\n🔥 Starting {num_workers} premium proxy workers...")
        start_time = time.time()
        
        # Execute with high concurrency (premium proxies can handle it)
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_to_worker = {
                executor.submit(self.premium_scrape_batch, task[0], task[1], task[2]): task[1] 
                for task in worker_tasks
            }
            
            results = []
            completed_workers = 0
            
            for future in as_completed(future_to_worker):
                worker_id = future_to_worker[future]
                try:
                    result = future.result()
                    results.append(result)
                    completed_workers += 1
                    
                    elapsed = time.time() - start_time
                    print(f"  📈 Worker {worker_id} finished ({completed_workers}/{num_workers}) - {elapsed:.1f}s elapsed")
                    
                except Exception as e:
                    print(f"  ❌ Worker {worker_id} failed: {e}")
        
        total_time = time.time() - start_time
        
        # Aggregate results
        total_successful = sum(r['successful'] for r in results)
        total_processed = sum(r['urls_processed'] for r in results)
        overall_rate = total_successful / total_time if total_time > 0 else 0
        overall_success_rate = (total_successful / total_processed) * 100 if total_processed > 0 else 0
        
        final_results = {
            'proxy_service': proxy_service,
            'demo_mode': self.demo_mode,
            'total_workers': num_workers,
            'total_urls': len(urls),
            'total_processed': total_processed,
            'total_successful': total_successful,
            'total_failed': total_processed - total_successful,
            'total_time_minutes': total_time / 60,
            'overall_rate': overall_rate,
            'success_percentage': overall_success_rate,
            'worker_results': results,
            'cost_estimate': self.calculate_cost_estimate(total_time, num_workers, proxy_service),
            'performance_vs_free': {
                'speed_improvement': overall_rate / 0.76 if overall_rate > 0 else 0,  # vs your baseline
                'time_saved_minutes': (len(urls) / 0.76 / 60) - (total_time / 60) if overall_rate > 0 else 0
            }
        }
        
        self.print_premium_results(final_results)
        
        # Save comprehensive results
        try:
            s3 = boto3.client('s3', region_name=self.aws_region)
            s3.put_object(
                Bucket=self.s3_bucket,
                Key=f'premium-benchmark-{proxy_service}-{int(time.time())}.json',
                Body=json.dumps(final_results, indent=2)
            )
            print(f"💾 Complete results saved to S3")
        except Exception as e:
            print(f"S3 save error: {e}")
        
        return final_results
    
    def calculate_cost_estimate(self, total_time_seconds, num_workers, proxy_service):
        """Calculate cost estimate for premium proxy usage"""
        # Pricing estimates (monthly plans converted to usage)
        pricing = {
            'smartproxy': {'monthly': 75, 'gb_included': 2, 'extra_gb': 15},
            'brightdata': {'monthly': 500, 'gb_included': 20, 'extra_gb': 10},
            'oxylabs': {'monthly': 300, 'gb_included': 10, 'extra_gb': 12}
        }
        
        # Estimate data usage (rough)
        estimated_mb_per_request = 0.5  # Conservative estimate
        total_requests = num_workers * (total_time_seconds / 60) * 0.5  # Rough request estimation
        estimated_gb = (total_requests * estimated_mb_per_request) / 1024
        
        if proxy_service in pricing:
            service_pricing = pricing[proxy_service]
            monthly_cost = service_pricing['monthly']
            
            # Simple usage-based estimate
            if estimated_gb <= service_pricing['gb_included']:
                estimated_cost = monthly_cost / 30  # Daily rate
            else:
                extra_gb = estimated_gb - service_pricing['gb_included']
                estimated_cost = (monthly_cost / 30) + (extra_gb * service_pricing['extra_gb'])
        else:
            estimated_cost = 50  # Default estimate
        
        return {
            'estimated_daily_cost': estimated_cost,
            'estimated_monthly_cost': monthly_cost if proxy_service in pricing else 200,
            'estimated_data_usage_gb': estimated_gb,
            'pricing_model': 'monthly_subscription_plus_usage'
        }
    
    def print_premium_results(self, results):
        """Print comprehensive premium results"""
        print(f"\n" + "=" * 60)
        print(f"🏁 PREMIUM PROXY BENCHMARK RESULTS")
        print(f"=" * 60)
        
        print(f"\n📊 PERFORMANCE:")
        print(f"   Service: {results['proxy_service'].title()} ({'Simulated' if results['demo_mode'] else 'Real'})")
        print(f"   Workers: {results['total_workers']}")
        print(f"   URLs: {results['total_successful']}/{results['total_processed']} ({results['success_percentage']:.1f}% success)")
        print(f"   Rate: {results['overall_rate']:.1f} URLs/second")
        print(f"   Time: {results['total_time_minutes']:.1f} minutes")
        
        print(f"\n⚡ VS YOUR BASELINE:")
        perf = results['performance_vs_free']
        print(f"   Speed Improvement: {perf['speed_improvement']:.1f}x faster")
        print(f"   Time Saved: {perf['time_saved_minutes']:.1f} minutes")
        
        print(f"\n💰 COST ESTIMATE:")
        cost = results['cost_estimate']
        print(f"   Daily Cost: ${cost['estimated_daily_cost']:.2f}")
        print(f"   Monthly Cost: ${cost['estimated_monthly_cost']}")
        print(f"   Data Usage: {cost['estimated_data_usage_gb']:.2f} GB")
        
        print(f"\n🎯 VERDICT:")
        if perf['speed_improvement'] > 10:
            print(f"   🚀 EXCELLENT - {perf['speed_improvement']:.0f}x speed improvement!")
            print(f"   💡 ROI: Worth it if you save >${cost['estimated_daily_cost']:.0f}/day in time")
        elif perf['speed_improvement'] > 5:
            print(f"   ✅ GOOD - {perf['speed_improvement']:.0f}x improvement")  
            print(f"   💡 Consider for regular high-volume scraping")
        else:
            print(f"   ⚠️  MARGINAL - Only {perf['speed_improvement']:.1f}x improvement")
            print(f"   💡 Stick with free methods unless time is critical")

def main():
    """Run premium proxy demonstration"""
    scraper = ProductionProxyScraper()
    
    # Load test URLs
    try:
        with open('url_cache.json', 'r') as f:
            all_urls = json.load(f)
        test_urls = all_urls[:100]  # Use 100 URLs for demo
        print(f"📂 Loaded {len(test_urls)} test URLs")
    except:
        print("❌ No URL cache found, please run smart-large-benchmark.py first")
        return
    
    print(f"🔥 PRODUCTION PREMIUM PROXY SCRAPER TEST")
    print(f"Note: Running in demo mode - simulating premium proxy performance")
    print(f"To use real proxies: Set credentials in .env and set demo_mode=False")
    
    # Test different proxy services
    services_to_test = ['smartproxy', 'brightdata']
    
    for service in services_to_test:
        print(f"\n{'='*60}")
        print(f"Testing {service.title()} Performance")
        print(f"{'='*60}")
        
        results = scraper.run_premium_benchmark(
            urls=test_urls,
            num_workers=8,  # High concurrency possible with premium
            proxy_service=service
        )
        
        time.sleep(2)  # Brief pause between tests

if __name__ == "__main__":
    main()