import os
import sys

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(dir_path, "./src"))

from shoppin_crawler.scraper import Scraper, ScraperException
from shoppin_crawler.crawler import ShoppinCrawler

async def scrape_one():
    async with Scraper() as scraper:
        async with scraper.scrape_links("https://www.flipkart.com/") as scraped_links:
            with open("scraper.json", "w+") as f:
                import json
                json.dump(list(scraped_links), f)

async def crawl_domains(domains: list[str]):
    crawler = ShoppinCrawler(
        domain_batch_size=2, 
        pages_batch_size=2,
        domain_timeout=100
    )
    product_urls = await crawler.crawl(set(domains), timeout=None)
    with open("crawler.json", "w+") as f:
        import json
        json.dump(product_urls, f)

if __name__ == "__main__":
    # Scrape urls from a given page
    # Given a parent domain, crawl
    # parallel efficient + identifying product urls
    import asyncio
    # asyncio.run(scrape_one())
    asyncio.run(crawl_domains([
        # "https://www.myntra.com/",
        "https://www.amazon.in/",
        # "https://www2.hm.com/",
        # "https://www.ajio.com/",
        "https://www.flipkart.com/"
    ]))