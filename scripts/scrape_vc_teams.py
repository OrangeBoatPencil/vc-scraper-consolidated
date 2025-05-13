#!/usr/bin/env python3
"""
VC Team Member Scraper
Scrapes team members from VC firm and portfolio company websites
"""
import asyncio
import argparse
import sys
import logging
from pathlib import Path
from typing import Dict, Any, List

# Add lib to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.utils.logging_config import setup_logging, get_logger
from lib.utils.config import Config
from lib.scrapers.team_scraper import TeamScraper
from lib.cleaning.data_cleaner import DataCleaner
from lib.database.supabase_client import SupabaseClient


async def scrape_team_page(url: str, site_name: str = None, config: Config = None) -> Dict[str, Any]:
    """
    Scrape team members from a single team page
    
    Args:
        url: Team page URL
        site_name: Name of the company/site (optional)
        config: Configuration object
        
    Returns:
        Dictionary with scraping results
    """
    logger = get_logger(__name__)
    scraper = None
    db_client = None
    
    try:
        # Initialize components
        scraper = TeamScraper(config.scraping)
        await scraper.start()
        
        db_client = SupabaseClient(config.database)
        cleaner = DataCleaner(base_url=url)
        
        # Ensure site exists in database
        site = await db_client.ensure_site_exists(site_name or url, url)
        if not site:
            raise Exception("Failed to create/find site in database")
        
        site_id = site['id']
        logger.info(f"Scraping team from: {url} (Site ID: {site_id})")
        
        # Scrape raw data
        raw_members = await scraper.scrape_team_page(url)
        logger.info(f"Scraped {len(raw_members)} raw team members")
        
        # Clean data
        cleaned_members = []
        for member in raw_members:
            cleaned = cleaner.clean_team_member(member)
            if cleaned and cleaner.validate_team_member(cleaned):
                cleaned_members.append(cleaned)
            else:
                logger.debug(f"Filtered out invalid member: {member.get('name', 'Unknown')}")
        
        logger.info(f"Cleaned and validated {len(cleaned_members)} team members")
        
        # Store in database with change tracking
        saved_count = await db_client.upsert_team_members_with_change_tracking(site_id, cleaned_members)
        
        # Update site's last scraped timestamp
        await db_client.update_site_last_scraped(site_id)
        
        return {
            "status": "success",
            "url": url,
            "site_name": site_name or url,
            "site_id": site_id,
            "total_scraped": len(raw_members),
            "total_cleaned": len(cleaned_members),
            "total_saved": saved_count,
            "members": cleaned_members
        }
        
    except Exception as e:
        logger.error(f"Error scraping team from {url}: {e}", exc_info=True)
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


async def scrape_teams_from_companies(config: Config, max_companies: int = None) -> Dict[str, Any]:
    """
    Scrape team pages from portfolio companies
    
    Args:
        config: Configuration object
        max_companies: Maximum number of companies to process (for testing)
        
    Returns:
        Dictionary with results
    """
    logger = get_logger(__name__)
    db_client = None
    
    try:
        # Get list of portfolio companies from database
        db_client = SupabaseClient(config.database)
        
        # Query portfolio companies with websites
        response = db_client.client.table('portfolio_companies')\
            .select('id, name, website, site_id')\
            .not_.is_('website', 'null')\
            .order('last_seen_at', desc=True)\
            .execute()
        
        companies = response.data
        if max_companies:
            companies = companies[:max_companies]
        
        logger.info(f"Found {len(companies)} portfolio companies with websites")
        
        results = []
        total_saved = 0
        
        # Try common team page patterns for each company
        team_url_patterns = [
            "/team",
            "/about",
            "/about-us",
            "/our-team",
            "/people",
            "/founders",
            "/leadership",
            "/company",
        ]
        
        for i, company in enumerate(companies):
            company_name = company['name']
            company_website = company['website']
            
            logger.info(f"Processing company {i+1}/{len(companies)}: {company_name}")
            
            # Try each team URL pattern
            team_scraped = False
            for pattern in team_url_patterns:
                team_url = company_website.rstrip('/') + pattern
                
                try:
                    logger.debug(f"Trying team URL: {team_url}")
                    result = await scrape_team_page(team_url, company_name, config)
                    
                    if result["status"] == "success" and result["total_saved"] > 0:
                        logger.info(f"✅ Found team page for {company_name}: {team_url}")
                        result['company_id'] = company['id']
                        result['company_name'] = company_name
                        results.append(result)
                        total_saved += result["total_saved"]
                        team_scraped = True
                        break
                        
                except Exception as e:
                    logger.debug(f"Failed to scrape {team_url}: {e}")
                    continue
            
            if not team_scraped:
                logger.info(f"❌ No team page found for {company_name}")
                results.append({
                    "status": "not_found",
                    "company_id": company['id'],
                    "company_name": company_name,
                    "company_website": company_website,
                    "total_saved": 0
                })
            
            # Rate limiting between companies
            if i < len(companies) - 1:
                delay = config.scraping.request_delay
                logger.debug(f"Waiting {delay} seconds before next company...")
                await asyncio.sleep(delay)
        
        # Calculate summary statistics
        successful = sum(1 for r in results if r["status"] == "success")
        not_found = sum(1 for r in results if r["status"] == "not_found")
        failed = len(results) - successful - not_found
        
        return {
            "status": "completed",
            "companies_processed": len(companies),
            "teams_found": successful,
            "teams_not_found": not_found,
            "teams_failed": failed,
            "total_members_saved": total_saved,
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Error in batch team scraping: {e}", exc_info=True)
        return {
            "status": "error",
            "error": str(e)
        }
    finally:
        if db_client:
            await db_client.close()


async def scrape_all_vc_teams(config: Config) -> Dict[str, Any]:
    """
    Scrape team pages from all configured VC firms
    
    Args:
        config: Configuration object
        
    Returns:
        Dictionary with results
    """
    logger = get_logger(__name__)
    sites = config.get_vc_sites()
    
    if not sites:
        logger.warning("No active VC sites configured")
        return {"status": "error", "error": "No active sites configured"}
    
    logger.info(f"Scraping team pages from {len(sites)} VC sites")
    
    results = []
    total_saved = 0
    
    for i, site in enumerate(sites):
        if not site.team_url:
            logger.info(f"Skipping {site.name} - no team URL configured")
            continue
            
        logger.info(f"Processing VC site {i+1}/{len(sites)}: {site.name}")
        
        # Scrape VC team page
        result = await scrape_team_page(site.team_url, site.name, config)
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
        "vc_sites_processed": len([s for s in sites if s.team_url]),
        "vc_sites_successful": successful,
        "vc_sites_failed": failed,
        "total_members_saved": total_saved,
        "results": results
    }


def main():
    """Main function with CLI argument parsing"""
    parser = argparse.ArgumentParser(
        description="VC Team Member Scraper",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Add arguments
    parser.add_argument('--url', 
                        help='Specific team page URL to scrape')
    parser.add_argument('--site-name', 
                        help='Name of the company/site (used with --url)')
    parser.add_argument('--all-sites', 
                        action='store_true',
                        help='Scrape team pages from all configured VC sites')
    parser.add_argument('--portfolio-companies', 
                        action='store_true',
                        help='Scrape team pages from portfolio companies')
    parser.add_argument('--max-companies', 
                        type=int,
                        help='Maximum number of portfolio companies to process (for testing)')
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
    
    args = parser.parse_args()
    
    # Validate arguments
    if not any([args.all_sites, args.portfolio_companies, args.url]):
        parser.error("Must specify one of: --url, --all-sites, or --portfolio-companies")
    
    if sum(bool(x) for x in [args.all_sites, args.portfolio_companies, args.url]) > 1:
        parser.error("Cannot specify multiple scraping modes")
    
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
        logger.info("Starting VC Team Member Scraper")
        logger.info(f"Configuration: {args}")
        
        # Run the appropriate operation
        if args.url:
            logger.info(f"Scraping single team page: {args.url}")
            result = asyncio.run(scrape_team_page(args.url, args.site_name, config))
        elif args.all_sites:
            logger.info("Scraping all configured VC team pages")
            result = asyncio.run(scrape_all_vc_teams(config))
        elif args.portfolio_companies:
            logger.info("Scraping team pages from portfolio companies")
            result = asyncio.run(scrape_teams_from_companies(config, args.max_companies))
        
        # Print results
        if result["status"] in ["success", "completed"]:
            logger.info("✅ Scraping completed successfully")
            
            if args.url:
                logger.info(f"Team members saved: {result['total_saved']}")
            elif args.all_sites:
                logger.info(f"VC sites processed: {result['vc_sites_processed']}")
                logger.info(f"Successful: {result['vc_sites_successful']}")
                logger.info(f"Failed: {result['vc_sites_failed']}")
                logger.info(f"Total members saved: {result['total_members_saved']}")
            elif args.portfolio_companies:
                logger.info(f"Companies processed: {result['companies_processed']}")
                logger.info(f"Teams found: {result['teams_found']}")
                logger.info(f"Teams not found: {result['teams_not_found']}")
                logger.info(f"Teams failed: {result['teams_failed']}")
                logger.info(f"Total members saved: {result['total_members_saved']}")
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
