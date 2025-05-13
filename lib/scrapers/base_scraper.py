"""
Base scraper class with common functionality
"""
import asyncio
import time
import random
from typing import Dict, List, Any, Optional, Union
from abc import ABC, abstractmethod
import logging
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page, Browser

from ..utils.config import ScrapingConfig
from ..utils.retry_logic import with_retry, RateLimitError, ScrapingError, CircuitBreaker

logger = logging.getLogger(__name__)

class BaseScraper(ABC):
    """Base class for all scrapers with common functionality"""
    
    def __init__(self, config: ScrapingConfig = None):
        self.config = config or ScrapingConfig()
        self.session: Optional[aiohttp.ClientSession] = None
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self._request_count = 0
        self._start_time = time.time()
        
        # Initialize circuit breakers for different failure types
        self._http_circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            timeout=300.0,  # 5 minutes
            expected_exception=(aiohttp.ClientError, asyncio.TimeoutError, RateLimitError)
        )
        
        self._playwright_circuit_breaker = CircuitBreaker(
            failure_threshold=3,
            timeout=600.0,  # 10 minutes
            expected_exception=(Exception,)  # More general for browser issues
        )
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()
    
    async def start(self):
        """Initialize scraper resources"""
        # Initialize aiohttp session
        connector = aiohttp.TCPConnector(limit=self.config.max_concurrent_requests)
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        headers = {'User-Agent': self.config.user_agent}
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=headers
        )
        
        logger.info("Scraper initialized")
    
    async def close(self):
        """Clean up scraper resources"""
        if self.session:
            await self.session.close()
        
        if self.page:
            await self.page.close()
        
        if self.browser:
            await self.browser.close()
        
        logger.info("Scraper closed")
    
    async def _get_playwright_browser(self) -> Browser:
        """Get or create Playwright browser instance"""
        if not self.browser:
            playwright = await async_playwright().start()
            self.browser = await playwright.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-extensions',
                ]
            )
        return self.browser
    
    async def _get_playwright_page(self) -> Page:
        """Get or create Playwright page instance"""
        if not self.page:
            browser = await self._get_playwright_browser()
            self.page = await browser.new_page()
            
            # Set user agent
            await self.page.set_user_agent(self.config.user_agent)
            
            # Set viewport
            await self.page.set_viewport_size({'width': 1920, 'height': 1080})
            
            # Block unnecessary resources for faster loading
            await self.page.route('**/*.{png,jpg,jpeg,gif,svg,css,font,woff,woff2}', 
                                 lambda route: route.abort())
        
        return self.page
    
    @with_retry(max_attempts=3, exceptions=(aiohttp.ClientError, asyncio.TimeoutError))
    async def fetch_with_requests(self, url: str, headers: Dict[str, str] = None) -> BeautifulSoup:
        """
        Fetch page content using aiohttp requests with circuit breaker
        
        Args:
            url: URL to fetch
            headers: Additional headers
            
        Returns:
            BeautifulSoup object
        """
        @self._http_circuit_breaker
        async def _fetch():
            await self._rate_limit_delay()
            
            request_headers = {}
            if headers:
                request_headers.update(headers)
            
            async with self.session.get(url, headers=request_headers) as response:
                # Check for rate limiting
                if response.status == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    await asyncio.sleep(retry_after)
                    raise RateLimitError(f"Rate limited on {url}")
                
                # Raise for other HTTP errors
                response.raise_for_status()
                
                content = await response.text()
                return BeautifulSoup(content, 'html.parser')
        
        return await _fetch()
    
    @with_retry(max_attempts=3, exceptions=(Exception,))
    async def fetch_with_playwright(
        self, 
        url: str, 
        wait_for: str = None,
        timeout: int = None
    ) -> BeautifulSoup:
        """
        Fetch page content using Playwright with circuit breaker
        
        Args:
            url: URL to fetch
            wait_for: CSS selector to wait for (optional)
            timeout: Custom timeout in milliseconds
            
        Returns:
            BeautifulSoup object
        """
        @self._playwright_circuit_breaker
        async def _fetch():
            await self._rate_limit_delay()
            
            page = await self._get_playwright_page()
            
            # Navigate to URL
            await page.goto(
                url, 
                wait_until='networkidle',
                timeout=timeout or self.config.timeout * 1000
            )
            
            # Wait for specific element if requested
            if wait_for:
                try:
                    await page.wait_for_selector(
                        wait_for, 
                        timeout=10000,
                        state='visible'
                    )
                except Exception as e:
                    logger.warning(f"Failed to wait for selector '{wait_for}': {e}")
            
            # Get page content
            content = await page.content()
            return BeautifulSoup(content, 'html.parser')
        
        return await _fetch()
    
    async def _rate_limit_delay(self):
        """Apply rate limiting delay between requests"""
        self._request_count += 1
        
        # Base delay
        if self.config.request_delay > 0:
            # Add some jitter to avoid thundering herd
            jitter = random.uniform(0.5, 1.5)
            delay = self.config.request_delay * jitter
            await asyncio.sleep(delay)
        
        # Additional delay based on request frequency
        elapsed = time.time() - self._start_time
        if elapsed > 0:
            rate = self._request_count / elapsed
            if rate > 10:  # More than 10 requests per second
                await asyncio.sleep(0.5)
    
    def parse_absolute_url(self, url: str, base_url: str) -> str:
        """Convert relative URLs to absolute URLs"""
        if not url:
            return ""
        
        # Already absolute
        if url.startswith(('http://', 'https://')):
            return url
        
        # Relative URL
        return urljoin(base_url, url)
    
    def extract_text(self, element, strip: bool = True) -> str:
        """
        Safely extract text from BeautifulSoup element
        
        Args:
            element: BeautifulSoup element
            strip: Whether to strip whitespace
            
        Returns:
            Extracted text or empty string
        """
        if not element:
            return ""
        
        text = element.get_text()
        return text.strip() if strip else text
    
    def extract_attr(self, element, attr: str, default: str = "") -> str:
        """
        Safely extract attribute from BeautifulSoup element
        
        Args:
            element: BeautifulSoup element
            attr: Attribute name
            default: Default value if attribute not found
            
        Returns:
            Attribute value or default
        """
        if not element:
            return default
        
        return element.get(attr, default)
    
    @abstractmethod
    async def scrape(self, url: str, **kwargs) -> List[Dict[str, Any]]:
        """
        Main scraping method - must be implemented by subclasses
        
        Args:
            url: URL to scrape
            **kwargs: Additional arguments
            
        Returns:
            List of scraped data dictionaries
        """
        pass
    
    def get_scraping_stats(self) -> Dict[str, Any]:
        """Get scraping statistics including circuit breaker status"""
        elapsed = time.time() - self._start_time
        return {
            'requests_made': self._request_count,
            'elapsed_time_seconds': elapsed,
            'requests_per_second': self._request_count / elapsed if elapsed > 0 else 0,
            'average_delay': elapsed / self._request_count if self._request_count > 0 else 0,
            'http_circuit_breaker_state': self._http_circuit_breaker.state,
            'playwright_circuit_breaker_state': self._playwright_circuit_breaker.state,
            'http_failures': self._http_circuit_breaker.failure_count,
            'playwright_failures': self._playwright_circuit_breaker.failure_count,
        }
    
    async def scrape_with_fallback(
        self, 
        url: str, 
        use_playwright: bool = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Scrape with automatic fallback between methods
        
        Args:
            url: URL to scrape
            use_playwright: Force Playwright usage (None = auto-detect)
            **kwargs: Additional scraping arguments
            
        Returns:
            Scraped data
        """
        # Auto-detect if Playwright is needed
        if use_playwright is None:
            # Check if URL likely needs JavaScript rendering
            parsed = urlparse(url)
            js_heavy_domains = ['google.', 'facebook.', 'linkedin.', 'github.']
            use_playwright = any(domain in parsed.netloc for domain in js_heavy_domains)
        
        try:
            if use_playwright:
                logger.debug(f"Using Playwright for {url}")
                soup = await self.fetch_with_playwright(url, **kwargs)
            else:
                logger.debug(f"Using requests for {url}")
                soup = await self.fetch_with_requests(url, **kwargs)
            
            return await self._parse_page(soup, url, **kwargs)
        
        except Exception as e:
            logger.warning(f"Primary scraping method failed for {url}: {e}")
            
            # Fallback to the other method
            try:
                if use_playwright:
                    logger.info(f"Falling back to requests for {url}")
                    soup = await self.fetch_with_requests(url, **kwargs)
                else:
                    logger.info(f"Falling back to Playwright for {url}")
                    soup = await self.fetch_with_playwright(url, **kwargs)
                
                return await self._parse_page(soup, url, **kwargs)
            
            except Exception as fallback_error:
                logger.error(f"Both scraping methods failed for {url}: {fallback_error}")
                raise ScrapingError(f"Failed to scrape {url}: {fallback_error}")
    
    @abstractmethod
    async def _parse_page(self, soup: BeautifulSoup, url: str, **kwargs) -> List[Dict[str, Any]]:
        """
        Parse the page content - must be implemented by subclasses
        
        Args:
            soup: BeautifulSoup object of the page
            url: Original URL
            **kwargs: Additional arguments
            
        Returns:
            List of parsed data dictionaries
        """
        pass
