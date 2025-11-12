# Scraper Benchmark Project

A comprehensive web scraping performance comparison between local and premium AWS infrastructure.

## Project Structure

```
scraper-benchmark/
├── src/                              # Core scraping implementations
│   ├── benchmark-s3.py              # Main scraper with S3 integration (8.9 URLs/sec)
│   ├── benchmark-framework.py       # Original framework comparison
│   └── benchmark-static.py          # Static scraping implementation
├── comprehensive-benchmark.py        # Full system comparison tool
├── production-premium-scraper.py     # Premium proxy implementation
├── benchmark-analysis.md             # Detailed performance analysis
├── requirements.txt                  # Python dependencies
├── .env.template                     # Configuration template
└── .env                             # Your configuration (keep private)
```

## Key Files

### Core Implementation
- **`src/benchmark-s3.py`** - Main production scraper achieving 8.9 URLs/sec with AWS S3 storage
- **`comprehensive-benchmark.py`** - System comparison tool with full CPU/memory monitoring
- **`production-premium-scraper.py`** - Premium proxy scraper for enterprise-scale (15x improvement)

### Analysis & Documentation  
- **`benchmark-analysis.md`** - Complete performance analysis and recommendations
- **`requirements.txt`** - All required dependencies for the project

### Configuration
- **`.env.template`** - Template for AWS and proxy service credentials
- **`.env`** - Your actual configuration (git-ignored)

## Performance Results

| Method | Rate (URLs/sec) | Success Rate | Best Use Case |
|--------|-----------------|-------------|---------------|
| Local Sequential | 6.4 | 88.8% | Small batches, development |
| Premium AWS | 98.0 | 95.6% | Production, high-volume |
| **Improvement** | **15.3x faster** | **+6.7%** | **Enterprise scraping** |

## Quick Start

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure credentials:**
   ```bash
   cp .env.template .env
   # Edit .env with your AWS credentials
   ```

3. **Run basic benchmark:**
   ```bash
   python src/benchmark-s3.py
   ```

4. **Run comprehensive comparison:**
   ```bash
   python comprehensive-benchmark.py
   ```

## Key Insights

- **Rate limiting is the primary bottleneck** for local scraping
- **Premium proxies enable true horizontal scaling** (15x improvement)
- **Different IP addresses eliminate bot detection** 
- **CPU efficiency improves dramatically** with proper infrastructure
- **ROI positive above 600 URLs/day** at $50/hour labor rate

## Production Recommendations

- **Small batches (<100 URLs):** Use `src/benchmark-s3.py`
- **High volume (>500 URLs/day):** Use `production-premium-scraper.py`
- **Development/testing:** Local approach with 4 workers
- **Production:** Premium with 12-16 workers

The project demonstrates that premium infrastructure provides exceptional ROI for regular scraping workloads with 15x speed improvement and higher reliability.