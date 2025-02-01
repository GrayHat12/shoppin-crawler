from shoppin_crawler.scraper import Scraper
from shoppin_crawler.logger import logger
import asyncio
from aiohttp import ClientSession, TraceConfig
from time import time
# from functools import lru_cache

_SHOULD_CRAWL_CACHE: dict[str, bool] = {}

class CrawlerException(Exception):
    pass

class SingleDomainCrawler:

    def __init__(self, domain: str):
        self.__session = None
        self.__domain = domain
        self.__queued_urls: set[str] = set()
        self.__visited_urls: set[str] = set()

    async def __is_product_link(self, url: str):
        if not await self.__should_crawl(url):
            return False
        not_subs = ["/account", "/login", "/logout", "/auth", "/api", "/signin", "/signup", "/ads"]
        expected_subs = ["/dp/", "/p/", "/product/", "/item/", "/buy/"]
        if any([sub in url for sub in not_subs]):
            return False
        if any([sub in url for sub in expected_subs]):
            return True
        return False
    
    # @lru_cache(maxsize=5000)
    async def __should_crawl(self, url: str):
        global _SHOULD_CRAWL_CACHE
        if url in _SHOULD_CRAWL_CACHE:
            return _SHOULD_CRAWL_CACHE[url]
        not_subs = ["/account", "/login", "/logout", "/auth", "/api", "/signin", "/signup", "/ads"]
        logger.debug(f"Checking if should crawl {url=}")
        if any([sub in url for sub in not_subs]):
            _SHOULD_CRAWL_CACHE[url] = False
            return False
        if self.__domain not in url:
            _SHOULD_CRAWL_CACHE[url] = False
            return False
        if not self.__validate_state(True):
            raise CrawlerException("Crawler not ready")
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-US,en;q=0.9",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        }
        async with self.__session.head(url, headers=headers, timeout=10, allow_redirects=True) as response:
            logger.debug(f"Got response from {url=} {await response.text()=} {response.status=} {response.headers.get('content-type', None)=}")
            _SHOULD_CRAWL_CACHE[url] = response.status in [200, 405] and "text/html" in response.headers.get("content-type", "")
            return _SHOULD_CRAWL_CACHE[url]
    
    async def __crawl_url(self, scraper: Scraper, url: str):
        logger.debug(f"Inside crawl_url {url=}")
        if url in self.__visited_urls:
            return False
        
        self.__visited_urls.add(url)
        
        if not await self.__should_crawl(url):
            # raise CrawlerException(f"Invalid {url=}")
            return False
        
        logger.debug(f"going to scrape {url=}")
        async with scraper.scrape_links(url) as links:
            logger.debug(f"Got {links=} from {url=}")
            self.__queued_urls = self.__queued_urls.union(links)
        return True
    
    async def crawl_domain(self, batch_size = 10, timeout: int | None = 500) -> set[str]:
        logger.debug(f"Crawling domain {self.__domain=}")
        if not isinstance(batch_size, int):
            batch_size = 10
        if batch_size > 50 or batch_size < 1:
            batch_size = 10
        
        if self.__domain not in self.__visited_urls:
            self.__queued_urls.add(self.__domain)
        
        start_time = time()
        async with Scraper() as scraper:
            while len(self.__queued_urls) > 0:
                logger.info(f"Inside while loop {batch_size=} {self.__queued_urls=} {self.__visited_urls=}")
                curr_time = time()
                logger.debug(f"heree {start_time+timeout=} {curr_time=}")
                if isinstance(timeout, int) and (start_time+timeout) < curr_time:
                    logger.info("Exiting due to timeout")
                    break
                logger.debug(f"here")
                task_batch = []
                for _ in range(batch_size):
                    logger.debug(f"{self.__queued_urls=}")
                    if len(self.__queued_urls) > 0:
                        url = self.__queued_urls.pop()
                        task_batch.append(self.__crawl_url(scraper, url))
                logger.debug(f"{len(task_batch)=}")
                if len(task_batch) == 0:
                    continue
                logger.debug(f"Running page batch of size {len(task_batch)=}")
                _ = await asyncio.gather(*task_batch, return_exceptions=True)
                logger.debug(f"Responses from batch {_=}")
        
        product_urls: set[str] = set()
        union = list(self.__visited_urls.union(self.__queued_urls))
        for url, is_product_link in zip(union, await asyncio.gather(*[self.__is_product_link(url) for url in union], return_exceptions=True)):
            if isinstance(is_product_link, bool) and is_product_link:
                product_urls.add(url)
        logger.debug(f"Done crawling {self.__domain=}. Returning {product_urls=}")
        return product_urls
    
    def __validate_state(self, ready: bool):
        return isinstance(self.__session, ClientSession) == ready

    async def __reset(self):
        if isinstance(self.__session, ClientSession):
            await self.__session.close()
    
    async def __initialise(self):
        await self.__reset()
        trace_config = TraceConfig()
        async def on_request_start(session, context, params):
            logger.debug(f'<aiohttp.client> Starting request <{params}>')
        async def on_request_end(session, context, params):
            logger.debug(f'<aiohttp.client> Ending request <{params}>')
        # trace_config.on_request_start.append(on_request_start)
        # trace_config.on_request_end.append(on_request_end)
        self.__session = ClientSession(trace_configs=[trace_config])
    
    async def __aenter__(self):
        await self.__initialise()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.__reset()
    
    async def start(self):
        await self.__initialise()
        return self

    async def close(self) -> None:
        return await self.__reset()

class ShoppinCrawler:
    
    def __init__(self, domain_batch_size: int = 10, pages_batch_size: int = 10, domain_timeout: int | None = 500):
        self.__domain_batch_size = domain_batch_size
        self.__pages_batch_size = pages_batch_size
        self.__domain_timeout = domain_timeout

        if not isinstance(self.__domain_batch_size, int):
            self.__domain_batch_size = 10
        if not isinstance(self.__pages_batch_size, int):
            self.__pages_batch_size = 10
        if not isinstance(self.__domain_timeout, int) and domain_timeout is not None:
            self.__domain_timeout = 500
        
        if self.__domain_batch_size < 1 or self.__domain_batch_size > 20:
            self.__domain_batch_size = 10
        if self.__pages_batch_size < 1 or self.__pages_batch_size > 20:
            self.__pages_batch_size = 10
        if self.__domain_timeout < 1:
            self.__domain_timeout = 500
    
    async def __crawl_domain(self, domain: str):
        async with SingleDomainCrawler(domain) as domain_crawler:
            return {domain: list(await domain_crawler.crawl_domain(self.__pages_batch_size, self.__domain_timeout))}

    async def crawl(self, domains: set[str], timeout: int | None = 500):
        pending_domains = set().union(domains)
        start_time = time()
        final_product_urls: dict[str, list[str]] = {}
        while len(pending_domains) > 0:
            curr_time = time()
            if isinstance(timeout, int) and (start_time+timeout) < curr_time:
                break
            task_batch = []
            for _ in range(self.__domain_batch_size):
                if len(pending_domains) > 0:
                    domain = pending_domains.pop()
                    task_batch.append(self.__crawl_domain(domain))
            if len(task_batch) == 0:
                continue
            for domain_responses in await asyncio.gather(*task_batch, return_exceptions=True):
                if isinstance(domain_responses, (Exception, BaseException)):
                    logger.error(f"Error {domain_responses=}")
                    continue
                final_product_urls.update(domain_responses)
                # {domain: [list of prod urls], domain2: [...]}
        return final_product_urls