"""
Portfolio company scraper with enhanced logic from V1 and circuit breaker integration
"""
import re
from typing import Dict, List, Any, Optional
import logging
from bs4 import BeautifulSoup, Tag

from .base_scraper import BaseScraper
from ..utils.config import ScrapingConfig
from ..utils.retry_logic import with_retry, CircuitBreaker

logger = logging.getLogger(__name__)

class PortfolioScraper(BaseScraper):
    """Scraper for VC firm portfolio companies with circuit breaker protection"""
    
    def __init__(self, config: ScrapingConfig = None):
        super().__init__(config)
        
        # Common selectors for portfolio companies
        self.company_selectors = [
            '.portfolio-item',
            '.portfolio-company',
            '.company-card',
            '.portfolio-grid-item',
            '.portfolio-list-item',
            '.company-item',
            '[data-company]',
            '.portfolio .company',
            '.investment-item',
            '.fund-portfolio-item',
        ]
        
        # Common selectors for company details within each item
        self.detail_selectors = {
            'name': [
                '.company-name',
                '.portfolio-company-name',
                '.company-title',
                '.portfolio-title',
                '.name',
                'h2',
                'h3',
                'h4',
                '[data-name]',
            ],
            'description': [
                '.company-description',
                '.description',
                '.summary',
                '.company-summary',
                '.overview',
                '.bio',
                'p',
                '.excerpt',
            ],
            'sector': [
                '.sector',
                '.industry',
                '.category',
                '.vertical',
                '.segment',
                '.focus-area',
                '.tag',
                '.company-category',
            ],
            'url': [
                'a[href]',
                '.company-link',
                '.website-link',
                '[data-url]',
            ],
            'logo': [
                'img',
                '.logo img',
                '.company-logo img',
                '.portfolio-logo img',
            ],
            'funding': [
                '.funding',
                '.investment',
                '.round',
                '.funding-round',
                '.investment-stage',
                '.stage',
            ],
            'location': [
                '.location',
                '.city',
                '.geography',
                '.region',
                '.headquarters',
            ]
        }
        
        # Initialize circuit breaker for portfolio page fetching
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            timeout=300.0,  # 5 minutes
            expected_exception=Exception
        )
    
    @CircuitBreaker(failure_threshold=3, timeout=60.0)
    async def scrape(self, url: str, **kwargs) -> List[Dict[str, Any]]:
        """
        Main scraping method for portfolio companies with circuit breaker protection
        
        Args:
            url: Portfolio page URL
            **kwargs: Additional arguments
            
        Returns:
            List of company dictionaries
        """
        return await self.scrape_with_fallback(url, **kwargs)
    
    @with_retry(max_attempts=3, exceptions=(Exception,))
    async def scrape_portfolio_page(self, url: str, **kwargs) -> List[Dict[str, Any]]:
        """
        Scrape portfolio companies from a VC firm's portfolio page
        
        Args:
            url: Portfolio page URL
            **kwargs: Additional arguments
            
        Returns:
            List of company dictionaries
        """
        return await self.scrape(url, **kwargs)
    
    async def _parse_page(self, soup: BeautifulSoup, url: str, **kwargs) -> List[Dict[str, Any]]:
        """
        Parse portfolio page and extract company information
        
        Args:
            soup: BeautifulSoup object
            url: Original URL
            **kwargs: Additional arguments
            
        Returns:
            List of company dictionaries
        """
        companies = []
        
        # Try different selectors to find portfolio companies
        company_elements = []
        for selector in self.company_selectors:
            elements = soup.select(selector)
            if elements and len(elements) > 1:  # We expect multiple companies
                company_elements = elements
                logger.debug(f"Found {len(elements)} companies using selector: {selector}")
                break
        
        if not company_elements:
            # Fallback: try to find any container with multiple links
            logger.warning(f"No companies found with standard selectors. Trying fallback approach.")
            company_elements = self._fallback_company_detection(soup)
        
        # Extract information from each company element
        for element in company_elements:
            try:
                company_data = await self._extract_company_info(element, url)
                if company_data and company_data.get('name'):
                    companies.append(company_data)
            except Exception as e:
                logger.error(f"Error extracting company info: {e}")
                continue
        
        logger.info(f"Extracted {len(companies)} companies from {url}")
        return companies
    
    def _fallback_company_detection(self, soup: BeautifulSoup) -> List[Tag]:
        """
        Fallback method to detect companies when standard selectors fail
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            List of potential company elements
        """
        # Look for grids or lists with multiple items
        grid_containers = soup.select('.grid, .row, .portfolio, .companies, .investments')
        
        for container in grid_containers:
            # Look for containers with multiple links or divs
            items = container.select('a, div')
            if len(items) > 5:  # Reasonable threshold for a portfolio
                # Filter items that look like companies
                company_items = []
                for item in items:
                    # Check if item has company-like text or structure
                    text = item.get_text().strip()
                    if len(text) > 10 and len(text) < 200:  # Reasonable company name/description length
                        if any(word in text.lower() for word in ['inc', 'corp', 'llc', 'ltd', 'co.']):
                            company_items.append(item)
                
                if len(company_items) > 3:
                    return company_items
        
        # Final fallback: just get all links that might be companies
        links = soup.select('a[href]')
        return [link for link in links if self._looks_like_company_link(link)]
    
    def _looks_like_company_link(self, element: Tag) -> bool:
        """
        Determine if an element looks like a company link
        
        Args:
            element: BeautifulSoup element
            
        Returns:
            Boolean indicating if element looks like a company
        """
        text = element.get_text().strip()
        href = element.get('href', '')
        
        # Skip navigation and footer links
        if any(word in text.lower() for word in ['home', 'about', 'contact', 'team', 'news', 'blog']):
            return False
        
        # Skip very short or very long text
        if len(text) < 3 or len(text) > 100:
            return False
        
        # Check if link points to an external site (likely a portfolio company)
        if href.startswith('http'):
            return True
        
        # Check if it has company-like words
        if any(word in text.lower() for word in ['inc', 'corp', 'llc', 'ltd', 'co.', '.com']):
            return True
        
        return False
    
    @CircuitBreaker(failure_threshold=5, timeout=60.0)
    async def _extract_company_info(self, element: Tag, base_url: str) -> Dict[str, Any]:
        """
        Extract company information from a portfolio item element with circuit breaker protection
        
        Args:
            element: BeautifulSoup element containing company info
            base_url: Base URL for resolving relative links
            
        Returns:
            Dictionary with company information
        """
        company = {}
        
        # Extract name
        company['name'] = self._extract_field(element, 'name')
        
        # Extract description
        company['description'] = self._extract_field(element, 'description')
        
        # Extract sector/industry
        company['sector'] = self._extract_field(element, 'sector')
        
        # Extract website URL
        company_url = self._extract_url(element, base_url)
        company['url'] = company_url
        
        # Extract logo
        logo_url = self._extract_logo(element, base_url)
        company['logo'] = logo_url
        
        # Extract funding information
        company['funding'] = self._extract_field(element, 'funding')
        
        # Extract location
        company['location'] = self._extract_field(element, 'location')
        
        # Try to extract more details if we have a link
        if company_url and company.get('name'):
            try:
                additional_info = await self._extract_additional_info(element)
                company.update(additional_info)
            except Exception as e:
                logger.debug(f"Could not extract additional info: {e}")
        
        # Add metadata
        company['source_url'] = base_url
        company['scraped_at'] = self._get_timestamp()
        
        return company
    
    def _extract_field(self, element: Tag, field: str) -> Optional[str]:
        """
        Extract a specific field from an element using multiple selectors
        
        Args:
            element: BeautifulSoup element
            field: Field name ('name', 'description', etc.)
            
        Returns:
            Extracted text or None
        """
        selectors = self.detail_selectors.get(field, [])
        
        for selector in selectors:
            found = element.select_one(selector)
            if found:
                text = self.extract_text(found)
                if text and len(text.strip()) > 0:
                    return text.strip()
        
        # Fallback: check if the element itself contains the information
        if field == 'name':
            # For name, try the element's title or alt attributes
            for attr in ['title', 'alt', 'data-name']:
                value = element.get(attr)
                if value:
                    return value.strip()
        
        return None
    
    def _extract_url(self, element: Tag, base_url: str) -> Optional[str]:
        """
        Extract company website URL
        
        Args:
            element: BeautifulSoup element
            base_url: Base URL for resolving relative links
            
        Returns:
            Company URL or None
        """
        # Try to find a direct link to the company
        for selector in self.detail_selectors['url']:
            link = element.select_one(selector)
            if link:
                href = link.get('href')
                if href:
                    # Check if it's an external link (likely the company website)
                    if href.startswith('http'):
                        return href
                    elif not href.startswith('/'):  # Relative link without leading slash
                        return href
        
        # Check if the element itself is a link
        if element.name == 'a':
            href = element.get('href')
            if href:
                if href.startswith('http'):
                    return href
                else:
                    return self.parse_absolute_url(href, base_url)
        
        return None
    
    def _extract_logo(self, element: Tag, base_url: str) -> Optional[str]:
        """
        Extract company logo URL
        
        Args:
            element: BeautifulSoup element
            base_url: Base URL for resolving relative URLs
            
        Returns:
            Logo URL or None
        """
        for selector in self.detail_selectors['logo']:
            img = element.select_one(selector)
            if img:
                src = img.get('src') or img.get('data-src') or img.get('data-lazy-src')
                if src:
                    return self.parse_absolute_url(src, base_url)
        
        return None
    
    async def _extract_additional_info(self, element: Tag) -> Dict[str, Any]:
        """
        Extract additional information that might be embedded in the element
        
        Args:
            element: BeautifulSoup element
            
        Returns:
            Dictionary with additional info
        """
        additional_info = {}
        
        # Look for data attributes
        for attr in element.attrs:
            if attr.startswith('data-'):
                key = attr.replace('data-', '').replace('-', '_')
                additional_info[key] = element[attr]
        
        # Try to extract structured data if present
        json_ld = element.select('script[type="application/ld+json"]')
        if json_ld:
            try:
                import json
                data = json.loads(json_ld[0].string)
                if isinstance(data, dict):
                    additional_info.update(data)
            except:
                pass
        
        # Look for microdata
        if element.get('itemscope'):
            itemtype = element.get('itemtype', '')
            if 'Organization' in itemtype:
                # Extract schema.org Organization data
                for prop_element in element.select('[itemprop]'):
                    prop = prop_element.get('itemprop')
                    if prop:
                        additional_info[prop] = self.extract_text(prop_element)
        
        return additional_info
    
    def _get_timestamp(self) -> str:
        """Get current timestamp in ISO format"""
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).isoformat()
    
    def _clean_company_data(self, companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Clean and validate extracted company data
        
        Args:
            companies: List of raw company data
            
        Returns:
            List of cleaned company data
        """
        cleaned = []
        
        for company in companies:
            # Skip if no name
            if not company.get('name'):
                continue
            
            # Clean name
            name = company['name'].strip()
            if len(name) < 2:
                continue
            
            # Clean description
            if company.get('description'):
                company['description'] = self._clean_description(company['description'])
            
            # Validate URL
            if company.get('url'):
                company['url'] = self._validate_url(company['url'])
            
            cleaned.append(company)
        
        return cleaned
    
    def _clean_description(self, description: str) -> str:
        """
        Clean company description text
        
        Args:
            description: Raw description text
            
        Returns:
            Cleaned description
        """
        # Remove extra whitespace
        description = re.sub(r'\s+', ' ', description).strip()
        
        # Remove very short descriptions
        if len(description) < 20:
            return ""
        
        # Truncate very long descriptions
        if len(description) > 500:
            description = description[:497] + "..."
        
        return description
    
    def _validate_url(self, url: str) -> Optional[str]:
        """
        Validate and clean URL
        
        Args:
            url: URL to validate
            
        Returns:
            Cleaned URL or None if invalid
        """
        if not url:
            return None
        
        # Ensure URL has protocol
        if not url.startswith(('http://', 'https://')):
            # Skip relative URLs that are likely internal navigation
            if url.startswith('/'):
                return None
            url = 'https://' + url
        
        # Basic validation
        try:
            from urllib.parse import urlparse
            result = urlparse(url)
            if result.netloc:
                return url
        except:
            pass
        
        return None
