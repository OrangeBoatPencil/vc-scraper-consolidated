#!/usr/bin/env python3
"""
Example script demonstrating the updated VC scraper with circuit breakers.

This script shows how to use the simplified configuration (without MCP)
and the circuit breaker integration for improved resilience.
"""

import asyncio
import logging
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config import settings, get_active_sites
from lib.scrapers.portfolio_scraper import PortfolioScraper
from lib.database.supabase_client import SupabaseClient
from lib.cleaning.data_cleaner import DataCleaner

# Set up logging
logging.basicConfig(
    level=settings.log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """Main scraping workflow with circuit breaker demonstration."""
    
    logger.info("Starting VC scraping with circuit breaker integration")
    
    # Initialize components
    db_client = SupabaseClient()
    data_cleaner = DataCleaner()
    
    # Get active sites from configuration
    active_sites = get_active_sites()
    
    if not active_sites:
        logger.warning("No active sites configured. Please update config/sites.yaml")
        return
    
    # Initialize scraper with circuit breaker support
    async with PortfolioScraper() as scraper:
        for site in active_sites:
            logger.info(f"Scraping {site.name} - {site.portfolio_url}")
            
            try:
                # Scrape portfolio companies
                companies = await scraper.scrape(site.portfolio_url)
                logger.info(f"Raw companies scraped: {len(companies)}")
                
                # Clean data
                cleaned_companies = []
                for company in companies:
                    cleaned = data_cleaner.clean_portfolio_company(company)
                    if cleaned and cleaned.get('name'):
                        cleaned_companies.append(cleaned)
                
                logger.info(f"Cleaned companies: {len(cleaned_companies)}")
                
                # Store in database
                if cleaned_companies:
                    await db_client.store_portfolio_companies(site.name, cleaned_companies)
                    logger.info(f"Stored {len(cleaned_companies)} companies for {site.name}")
                
                # Get scraping statistics including circuit breaker status
                stats = scraper.get_scraping_stats()
                logger.info(f"Scraping stats for {site.name}:")
                logger.info(f"  - Requests made: {stats['requests_made']}")
                logger.info(f"  - HTTP circuit breaker: {stats['http_circuit_breaker_state']}")
                logger.info(f"  - Playwright circuit breaker: {stats['playwright_circuit_breaker_state']}")
                logger.info(f"  - HTTP failures: {stats['http_failures']}")
                
            except Exception as e:
                logger.error(f"Failed to scrape {site.name}: {e}")
                
                # Check circuit breaker status after failure
                stats = scraper.get_scraping_stats()
                logger.info(f"Circuit breaker status after failure:")
                logger.info(f"  - HTTP: {stats['http_circuit_breaker_state']}")
                logger.info(f"  - Playwright: {stats['playwright_circuit_breaker_state']}")
                
                continue
    
    logger.info("Scraping workflow completed")


async def test_circuit_breaker():
    """Test circuit breaker behavior with intentional failures."""
    
    logger.info("Testing circuit breaker behavior")
    
    from lib.scrapers.base_scraper import BaseScraper
    
    class TestScraper(BaseScraper):
        async def scrape(self, url: str, **kwargs):
            return await self.scrape_with_fallback(url, **kwargs)
        
        async def _parse_page(self, soup, url, **kwargs):
            return [{'test': 'data'}]
    
    async with TestScraper() as scraper:
        # Test with invalid URLs to trigger failures
        test_urls = [
            "https://invalid-domain-that-doesnt-exist.com",
            "https://httpstat.us/500",  # Returns 500 error
            "https://httpstat.us/429",  # Rate limit simulation
        ]
        
        for i, url in enumerate(test_urls):
            logger.info(f"Test attempt {i+1}: {url}")
            try:
                await scraper.scrape(url)
            except Exception as e:
                logger.info(f"Expected failure: {e}")
            
            # Check circuit breaker status
            stats = scraper.get_scraping_stats()
            logger.info(f"Circuit breaker status:")
            logger.info(f"  - HTTP: {stats['http_circuit_breaker_state']} (failures: {stats['http_failures']})")
            logger.info(f"  - Playwright: {stats['playwright_circuit_breaker_state']} (failures: {stats['playwright_failures']})")
            
            # Add delay between attempts
            await asyncio.sleep(1)


if __name__ == "__main__":
    # Validate configuration before starting
    try:
        settings.validate_required_settings()
        logger.info("✅ Configuration validated successfully")
    except ValueError as e:
        logger.error(f"❌ Configuration error: {e}")
        sys.exit(1)
    
    # Run the main scraping workflow
    # asyncio.run(main())
    
    # Uncomment to test circuit breaker behavior
    asyncio.run(test_circuit_breaker())
