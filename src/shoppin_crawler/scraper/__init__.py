# import os
# os.environ["DEBUG"] = "pw:api"
# import random
from asyncio import sleep
from contextlib import asynccontextmanager
from playwright.async_api import async_playwright, Playwright, Browser
from shoppin_crawler.logger import logger

class ScraperException(Exception):
    def __init__(self, *args):
        super().__init__(*args)

class Scraper:
    
    def __init__(self):
        self.__browser = None
        self.__playwright = None
    
    @asynccontextmanager
    async def scrape_links(self, url: str):
        # Validate state
        if not self.__validate_state(True):
            raise ScraperException("Scraper Not ready")
        
        # Open page
        page = await self.__browser.new_page()
        logger.debug("Opened new page")
        try:
            resp = await page.goto(url)
            if resp.status != 200:
                raise ScraperException(f"Failed call {resp.url=} {resp.status=} {resp.status_text=}")
            
            await page.wait_for_load_state("domcontentloaded")
            logger.debug("Dom content loaded")
            try:
                await page.wait_for_load_state("networkidle", timeout=60)
                logger.debug("Network idle")
            except:
                logger.warning("Failed to idle network after page load")

            # Scroll to bottom of page
            max_scroll = 10
            height = await page.evaluate("document.body.scrollHeight")
            for _ in range(max_scroll):
                logger.debug(f"Scrolling for {_}")
                scroll_y = await page.evaluate("document.body.scrollHeight")
                if scroll_y > height:
                    break
                await page.mouse.wheel(0, scroll_y)
                height = await page.evaluate("document.body.scrollHeight")
                await sleep(1)
                logger.debug("scrolled and slept")
                try:
                    await page.wait_for_load_state("networkidle", timeout=5)
                except:
                    logger.warning("Network not idle in specified time")
            
            # Scrape links
            links: set[str] = set()
            for link in await page.evaluate("Array.from(document.links).map(x => x.href)"):
                if isinstance(link, str):
                    links.add(link)
            
            yield links
        finally:
            await page.close()
    
    def __validate_state(self, ready: bool):
        return (isinstance(self.__playwright, Playwright) and isinstance(self.__browser, Browser)) == ready

    async def __reset(self):
        if self.__browser is not None:
            await self.__browser.close()
        if self.__playwright is not None:
            await self.__playwright.stop()
        self.__playwright = None
        self.__browser = None
    
    async def __initialise(self):
        await self.__reset()
        self.__playwright = await async_playwright().start()
        self.__browser = await self.__playwright.chromium.launch()
    
    async def __aenter__(self):
        await self.__initialise()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.__reset()
    
    async def start(self):
        await self.__initialise()
        return self

    async def close(self):
        return await self.__reset()
    