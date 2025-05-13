"""
Fortune Term Sheet scraper using regex-based extraction with circuit breaker integration
"""
import asyncio
import re
import json
from typing import Dict, List, Any, Optional, Set
from datetime import datetime, timezone
import logging
from bs4 import BeautifulSoup
import aiohttp
import feedparser

from .base_scraper import BaseScraper
from ..utils.config import ScrapingConfig
from ..utils.retry_logic import with_retry, CircuitBreaker

logger = logging.getLogger(__name__)

class FortuneScraper(BaseScraper):
    """Scraper for Fortune Term Sheet deals using regex-based extraction with circuit breaker protection"""
    
    def __init__(self, config: ScrapingConfig = None):
        super().__init__(config)
        
        # Fortune-specific URLs and patterns
        self.default_feed_url = "https://fortune.com/tag/term-sheet/feed/"
        self.base_url = "https://fortune.com"
        self.article_path_pattern = "/20"  # Articles start with /YYYY/
        
        # Initialize circuit breakers for different operations
        self.rss_circuit_breaker = CircuitBreaker(
            failure_threshold=3,
            timeout=180.0,  # 3 minutes
            expected_exception=Exception
        )
        
        self.article_circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            timeout=300.0,  # 5 minutes
            expected_exception=Exception
        )
    
    @CircuitBreaker(failure_threshold=3, timeout=120.0)
    async def scrape_rss_feed(self, feed_url: str = None) -> List[str]:
        """
        Scrape Fortune RSS feed to get latest article URLs with circuit breaker protection
        
        Args:
            feed_url: RSS feed URL (defaults to Fortune Term Sheet feed)
            
        Returns:
            List of article URLs
        """
        feed_url = feed_url or self.default_feed_url
        article_urls = []
        
        try:
            # Fetch RSS feed
            logger.info(f"Fetching RSS feed: {feed_url}")
            
            async with self.session.get(feed_url) as response:
                response.raise_for_status()
                feed_content = await response.text()
            
            # Parse RSS feed
            feed = feedparser.parse(feed_content)
            
            # Extract article URLs
            for entry in feed.entries:
                url = entry.get('link', '')
                if url and self.article_path_pattern in url:
                    article_urls.append(url)
            
            logger.info(f"Found {len(article_urls)} articles in RSS feed")
            return article_urls
            
        except Exception as e:
            logger.error(f"Error scraping RSS feed {feed_url}: {e}")
            return []
    
    @with_retry(max_attempts=3, exceptions=(Exception,))
    async def discover_new_articles(self, tag_url: str = None, processed_urls: Set[str] = None) -> List[str]:
        """
        Discover new articles from the Fortune Term Sheet tag page
        
        Args:
            tag_url: Tag page URL (defaults to Term Sheet tag)
            processed_urls: Set of already processed URLs
            
        Returns:
            List of new article URLs
        """
        tag_url = tag_url or "https://fortune.com/tag/term-sheet/"
        processed_urls = processed_urls or set()
        
        try:
            logger.info(f"Discovering articles from: {tag_url}")
            
            # Fetch the tag page
            soup = await self.fetch_with_requests(tag_url)
            
            # Find article links
            article_links = set()
            for link in soup.select('a[href]'):
                href = link.get('href')
                if not href:
                    continue
                
                # Convert to absolute URL
                abs_url = self.parse_absolute_url(href, tag_url)
                
                # Check if it looks like an article URL
                parsed = self._parse_url(abs_url)
                if parsed and self._is_fortune_article(parsed):
                    article_links.add(abs_url)
            
            # Filter out already processed URLs
            new_urls = sorted(list(article_links - processed_urls))
            logger.info(f"Found {len(article_links)} total articles, {len(new_urls)} are new")
            
            return new_urls
            
        except Exception as e:
            logger.error(f"Error discovering articles from {tag_url}: {e}")
            return []
    
    def _parse_url(self, url: str) -> Optional[Dict[str, str]]:
        """Parse URL into components"""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            return {
                'scheme': parsed.scheme,
                'netloc': parsed.netloc,
                'path': parsed.path,
                'query': parsed.query,
                'fragment': parsed.fragment
            }
        except:
            return None
    
    def _is_fortune_article(self, parsed_url: Dict[str, str]) -> bool:
        """Check if URL looks like a Fortune article"""
        if parsed_url['netloc'] != 'fortune.com':
            return False
        
        # Check if path starts with /YYYY/ (article year)
        if not parsed_url['path'].startswith(self.article_path_pattern):
            return False
        
        return True
    
    @CircuitBreaker(failure_threshold=3, timeout=60.0)
    async def scrape(self, url: str, **kwargs) -> List[Dict[str, Any]]:
        """
        Main scraping method for Fortune articles with circuit breaker protection
        
        Args:
            url: Article URL
            **kwargs: Additional arguments
            
        Returns:
            List of deals extracted from the article
        """
        return await self.extract_deals_from_article(url, **kwargs)
    
    async def _parse_page(self, soup: BeautifulSoup, url: str, **kwargs) -> List[Dict[str, Any]]:
        """
        Parse article page - not used for Fortune articles
        """
        # This method is required by the base class but not used for Fortune
        # We use extract_deals_from_article instead
        return []
    
    @with_retry(max_attempts=3, exceptions=(Exception,))
    async def extract_deals_from_article(self, article_url: str, **kwargs) -> List[Dict[str, Any]]:
        """
        Extract deals from a Fortune article using regex-based text mining
        
        Args:
            article_url: URL of the article
            **kwargs: Additional arguments
            
        Returns:
            List of extracted and cleaned deals
        """
        try:
            logger.info(f"Extracting deals from: {article_url}")
            
            # First, get the article content
            soup = await self.fetch_with_playwright(article_url)
            article_text = self._extract_article_text(soup)
            
            if not article_text:
                logger.warning(f"No article text found for {article_url}")
                return []
            
            # Extract deals using regex patterns
            extracted_data = self._extract_deals_with_regex(article_text, article_url)
            
            if not extracted_data:
                logger.warning(f"No deals extracted from {article_url}")
                return []
            
            # Clean and validate the extracted deals
            cleaned_deals = []
            article_date = extracted_data.get('article_publication_date')
            article_title = extracted_data.get('article_title')
            
            for deal in extracted_data.get('deals', []):
                # Add source information
                deal['source_article_url'] = article_url
                deal['source_article_title'] = article_title
                
                # Clean the deal data
                cleaned_deal = self._clean_deal_data(deal, article_date)
                if cleaned_deal:
                    cleaned_deals.append(cleaned_deal)
            
            logger.info(f"Extracted {len(cleaned_deals)} deals from {article_url}")
            return cleaned_deals
            
        except Exception as e:
            logger.error(f"Error extracting deals from {article_url}: {e}")
            return []
    
    def _extract_article_text(self, soup: BeautifulSoup) -> str:
        """
        Extract the main article text from Fortune page
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Article text
        """
        # Try different selectors for Fortune articles
        content_selectors = [
            '.article-content',
            '.post-content',
            '.entry-content',
            'article .content',
            '.article-body',
            '.post-body'
        ]
        
        for selector in content_selectors:
            content = soup.select_one(selector)
            if content:
                # Remove unwanted elements
                for element in content.select('script, style, figure, .advertisement'):
                    element.decompose()
                
                return content.get_text(strip=True)
        
        # Fallback: get text from main content area
        main_content = soup.select_one('main, .main, article')
        if main_content:
            return main_content.get_text(strip=True)
        
        return ""
    
    def _extract_deals_with_regex(self, article_text: str, article_url: str) -> Dict[str, Any]:
        """
        Extract deals using regex patterns
        
        Args:
            article_text: Article text
            article_url: Article URL
            
        Returns:
            Extracted data dictionary
        """
        deals = []
        
        # Look for venture deals section
        venture_deals_pattern = r'VENTURE DEALS?\s*:?\s*(.*?)(?=\n\n|\Z)'
        venture_section = re.search(venture_deals_pattern, article_text, re.IGNORECASE | re.DOTALL)
        
        if not venture_section:
            # Try other patterns
            patterns = [
                r'deal\s*roundup\s*:?\s*(.*?)(?=\n\n|\Z)',
                r'funding\s*news\s*:?\s*(.*?)(?=\n\n|\Z)',
                r'investment\s*deals\s*:?\s*(.*?)(?=\n\n|\Z)'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, article_text, re.IGNORECASE | re.DOTALL)
                if match:
                    venture_section = match
                    break
        
        if venture_section:
            deals_text = venture_section.group(1)
            
            # Extract individual deals
            # Look for patterns like "Company raised $X from Investor"
            deal_patterns = [
                r'([A-Z][a-zA-Z\s&\'.-]+?)\s+(?:raised|secured|closed)\s+\$([0-9.]+)([MB]?)\s+(?:in\s+)?([Aa]?\s?Series\s+[A-Z]|Seed|Pre-[Ss]eed|Angel)(?:\s+funding)?(?:\s+(?:from|led by)\s+(.+?))?(?=\.|,|\n|$)',
                r'([A-Z][a-zA-Z\s&\'.-]+?)\s+\$([0-9.]+)([MB]?)\s+([Aa]?\s?Series\s+[A-Z]|Seed|Pre-[Ss]eed|Angel)(?:\s+(?:from|led by)\s+(.+?))?(?=\.|,|\n|$)',
            ]
            
            for pattern in deal_patterns:
                matches = re.finditer(pattern, deals_text, re.IGNORECASE)
                
                for match in matches:
                    groups = match.groups()
                    
                    if len(groups) >= 4:
                        company_name = groups[0].strip()
                        amount = groups[1]
                        multiplier = groups[2].upper() if groups[2] else ''
                        round_type = groups[3].strip()
                        investors = groups[4].strip() if len(groups) > 4 and groups[4] else ''
                        
                        # Convert amount
                        amount_desc = f"${amount}{multiplier}"
                        
                        # Parse investors
                        lead_investor = None
                        other_investors = []
                        
                        if investors:
                            # Clean up the investor text
                            investors = re.sub(r'\s*\.$', '', investors)  # Remove trailing period
                            
                            # Split investors
                            investor_list = re.split(r',\s*|\s+and\s+', investors)
                            if investor_list:
                                lead_investor = investor_list[0].strip()
                                other_investors = [inv.strip() for inv in investor_list[1:] if inv.strip()]
                        
                        # Clean round type
                        round_type = re.sub(r'^[Aa]\s+', '', round_type)  # Remove leading "A"
                        
                        deal = {
                            'startup_name': company_name,
                            'funding_amount_description': amount_desc,
                            'round_type': round_type,
                            'lead_investor': lead_investor,
                            'other_investors': other_investors,
                            'summary': f"{company_name} raised {amount_desc} in {round_type}"
                        }
                        deals.append(deal)
        
        # Extract article metadata
        title_match = re.search(r'<title[^>]*>([^<]+)</title>', article_text, re.IGNORECASE)
        article_title = title_match.group(1).strip() if title_match else "Fortune Term Sheet"
        
        # Clean up title (remove common suffixes)
        article_title = re.sub(r'\s*\|\s*Fortune.*$', '', article_title, flags=re.IGNORECASE)
        
        # Try to extract date from URL or text
        date_match = re.search(r'/(\d{4})/(\d{2})/(\d{2})/', article_url)
        if date_match:
            year, month, day = date_match.groups()
            publication_date = f"{year}-{month}-{day}"
        else:
            # Try to find date in the article text
            date_patterns = [
                r'(\w+\s+\d{1,2},\s+\d{4})',  # "January 15, 2024"
                r'(\d{4}-\d{2}-\d{2})',       # "2024-01-15"
            ]
            publication_date = None
            for pattern in date_patterns:
                date_match = re.search(pattern, article_text)
                if date_match:
                    publication_date = date_match.group(1)
                    break
            
            if not publication_date:
                publication_date = datetime.now().strftime("%Y-%m-%d")
        
        return {
            'article_title': article_title,
            'article_url': article_url,
            'article_publication_date': publication_date,
            'deals': deals
        }
    
    def _clean_deal_data(self, deal: Dict[str, Any], article_date: Optional[str]) -> Optional[Dict[str, Any]]:
        """
        Clean and validate deal data using DataCleaner
        
        Args:
            deal: Raw deal data
            article_date: Article publication date
            
        Returns:
            Cleaned deal data or None if invalid
        """
        from ..cleaning.data_cleaner import DataCleaner
        
        # Create a data cleaner instance
        cleaner = DataCleaner()
        
        # Use the advanced cleaning method from DataCleaner
        cleaned = cleaner.clean_deal_data(deal, article_date)
        
        return cleaned
    
    async def process_fortune_deals(
        self, 
        article_urls: List[str] = None, 
        max_articles: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Process multiple Fortune articles to extract deals with circuit breaker protection
        
        Args:
            article_urls: List of article URLs (if None, uses RSS feed)
            max_articles: Maximum number of articles to process
            
        Returns:
            List of all extracted deals
        """
        if not article_urls:
            # Get URLs from RSS feed
            article_urls = await self.scrape_rss_feed()
            article_urls = article_urls[:max_articles]
        
        all_deals = []
        
        for i, url in enumerate(article_urls):
            try:
                logger.info(f"Processing article {i+1}/{len(article_urls)}: {url}")
                
                deals = await self.extract_deals_from_article(url)
                all_deals.extend(deals)
                
                # Rate limiting delay
                if i < len(article_urls) - 1:
                    await asyncio.sleep(self.config.request_delay)
                    
            except Exception as e:
                logger.error(f"Error processing article {url}: {e}")
                # Check if circuit breaker is open
                if hasattr(e, 'circuit_breaker') and e.circuit_breaker:
                    logger.warning("Circuit breaker is open, pausing article processing")
                    break
                continue
        
        logger.info(f"Total deals extracted from {len(article_urls)} articles: {len(all_deals)}")
        return all_deals
    
    def clean_extracted_deals(self, deals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Clean a list of extracted deals
        
        Args:
            deals: List of raw deal data
            
        Returns:
            List of cleaned deals
        """
        cleaned_deals = []
        
        for deal in deals:
            cleaned = self._clean_deal_data(deal, deal.get('article_publication_date'))
            if cleaned:
                cleaned_deals.append(cleaned)
        
        return cleaned_deals
