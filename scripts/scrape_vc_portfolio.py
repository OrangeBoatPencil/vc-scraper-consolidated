#!/usr/bin/env python3
"""
VC Portfolio Company Scraper
Scrapes portfolio companies from VC firm websites
"""
import asyncio
import argparse
import sys
import logging
from pathlib import Path
from typing import Dict, Any

# Add lib to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.utils.logging_config import setup_logging, get_logger
from lib.utils.config import Config
from lib.scrapers.portfolio_scraper import PortfolioScraper
from lib.cleaning.data_cleaner import DataCleaner
from lib.database.supabase_client import SupabaseClient


async def scrape_portfolio(url: str, site_name: str = None, config: Config = None) -> Dict[str, Any]:
    """
    Scrape portfolio companies from a single VC firm website
    
    Args:
        url: Portfolio page URL
        site_name: Name of the VC firm (optional)
        config: Configuration object
        
    Returns:
        Dictionary with scraping results
    """
    logger = get_logger(__name__)
    scraper = None
    db_client = None
    
    try:
        # Initialize components
        scraper = PortfolioScraper(config.scraping)
        await scraper.start()
        
        db_client = SupabaseClient(config.database)
        cleaner = DataCleaner(base_url=url)
        
        # Ensure site exists in database
        site = await db_client.ensure_site_exists(site_name or url, url)
        if not site:
            raise Exception("Failed to create/find site in database")
        
        site_id = site['id']
        logger.info(f"Scraping portfolio from: {url} (Site ID: {site_id})")
        
        # Scrape raw data
        raw_companies = await scraper.scrape_portfolio_page(url)
        logger.info(f"Scraped {len(raw_companies)} raw companies")
        
        # Clean data
        cleaned_companies = []
        for company in raw_companies:
            cleaned = cleaner.clean_portfolio_company(company)
            if cleaned and cleaner.validate_company_data(cleaned):
                cleaned_companies.append(cleaned)
            else:
                logger.debug(f"Filtered out invalid company: {company.get('name', 'Unknown')}")
        
        logger.info(f"Cleaned and validated {len(cleaned_companies)} companies")
        
        # Store in database with change tracking
        saved_count = await db_client.upsert_companies_with_change_tracking(site_id, cleaned_companies)
        
        # Update site's last scraped timestamp
        await db_client.update_site_last_scraped(site_id)
        
        return {
            "status": "success",
            "url": url,
            "site_name": site_name or url,
            "site_id": site_id,
            "total_scraped": len(raw_companies),
            "total_cleaned": len(cleaned_companies),
            "total_saved": saved_count,
            "companies": cleaned_companies
        }
        
    except Exception as e:
        logger.error(f"Error scraping portfolio from {url}: {e}", exc_info=True)
        return {
            "status": "error",
            "url": url,
            "error": str(e)
        }
        
    finally:
        if scraper:
            await scraper.close()
        if db_client:
            await db_client.close()


async def scrape_all_sites(config: Config) -> Dict[str, Any]:
    """
    Scrape all configured VC sites
    
    Args:
        config: Configuration object
        
    Returns:
        Dictionary with overall results
    """
    logger = get_logger(__name__)
    sites = config.get_vc_sites()
    
    if not sites:
        logger.warning("No active sites configured")
        return {"status": "error", "error": "No active sites configured"}
    
    logger.info(f"Scraping {len(sites)} VC sites")
    
    results = []
    total_saved = 0
    
    for i, site in enumerate(sites):
        logger.info(f"Processing site {i+1}/{len(sites)}: {site.name}")
        
        # Scrape individual site
        result = await scrape_portfolio(site.portfolio_url, site.name, config)
        results.append(result)
        
        if result["status"] == "success":
            total_saved += result["total_saved"]
        
        # Rate limiting between sites
        if i < len(sites) - 1:
            delay = config.scraping.request_delay
            logger.debug(f"Waiting {delay} seconds before next site...")
            await asyncio.sleep(delay)
    
    # Calculate summary statistics
    successful = sum(1 for r in results if r["status"] == "success")
    failed = len(results) - successful
    
    return {
        "status": "completed",
        "sites_processed": len(sites),
        "sites_successful": successful,
        "sites_failed": failed,
        "total_companies_saved": total_saved,
        "results": results
    }


def main():
    """Main function with CLI argument parsing"""
    parser = argparse.ArgumentParser(
        description="VC Portfolio Company Scraper",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Add arguments
    parser.add_argument('--url', 
                        help='Specific portfolio URL to scrape')
    parser.add_argument('--site-name', 
                        help='Name of the VC firm (used with --url)')
    parser.add_argument('--all-sites', 
                        action='store_true',
                        help='Scrape all configured VC sites')
    parser.add_argument('--config-file', 
                        help='Path to configuration file',
                        default=None)
    parser.add_argument('--log-level', 
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        default='INFO',
                        help='Logging level')
    parser.add_argument('--dry-run', 
                        action='store_true',
                        help='Run without saving to database')
    parser.add_argument('--max-pages', 
                        type=int,
                        help='Maximum number of companies to scrape (for testing)')
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.all_sites and not args.url:
        parser.error("Must specify either --url or --all-sites")
    
    if args.url and args.all_sites:
        parser.error("Cannot specify both --url and --all-sites")
    
    try:
        # Load configuration
        config = Config(config_file=args.config_file)
        
        # Setup logging
        setup_logging(
            level=args.log_level,
            enable_json=config.is_production(),
            sentry_config=config.get_sentry_config()
        )
        
        logger = get_logger(__name__)
        logger.info("Starting VC Portfolio Scraper")
        logger.info(f"Configuration: {args}")
        
        # Run the appropriate operation
        if args.all_sites:
            logger.info("Scraping all configured sites")
            result = asyncio.run(scrape_all_sites(config))
        else:
            logger.info(f"Scraping single site: {args.url}")
            result = asyncio.run(scrape_portfolio(args.url, args.site_name, config))
        
        # Print results
        if result["status"] == "success":
            logger.info("✅ Scraping completed successfully")
            if args.all_sites:
                logger.info(f"Sites processed: {result['sites_processed']}")
                logger.info(f"Successful: {result['sites_successful']}")
                logger.info(f"Failed: {result['sites_failed']}")
                logger.info(f"Total companies saved: {result['total_companies_saved']}")
            else:
                logger.info(f"Companies saved: {result['total_saved']}")
        else:
            logger.error("❌ Scraping failed")
            logger.error(f"Error: {result.get('error', 'Unknown error')}")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
