#!/usr/bin/env python3
"""
Fortune Term Sheet Deals Scraper
Scrapes funding deals from Fortune Term Sheet RSS feed and articles
"""
import asyncio
import argparse
import sys
import logging
from pathlib import Path
from typing import Dict, Any, List, Set
from datetime import datetime

# Add lib to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.utils.logging_config import setup_logging, get_logger
from lib.utils.config import Config
from lib.scrapers.fortune_scraper import FortuneScraper
from lib.database.supabase_client import SupabaseClient


def load_processed_urls(filepath: str) -> Set[str]:
    """Load previously processed URLs from file"""
    if not Path(filepath).exists():
        return set()
    
    with open(filepath, 'r') as f:
        return {line.strip() for line in f if line.strip()}


def save_processed_urls(filepath: str, urls: Set[str]):
    """Save processed URLs to file"""
    with open(filepath, 'w') as f:
        for url in sorted(urls):
            f.write(f"{url}\n")


async def scrape_fortune_rss(config: Config, max_articles: int = 10) -> Dict[str, Any]:
    """
    Scrape latest Fortune Term Sheet articles from RSS feed
    
    Args:
        config: Configuration object
        max_articles: Maximum number of articles to process
        
    Returns:
        Dictionary with scraping results
    """
    logger = get_logger(__name__)
    scraper = None
    db_client = None
    
    try:
        # Initialize components
        scraper = FortuneScraper(config.scraping)
        await scraper.start()
        
        db_client = SupabaseClient(config.database)
        
        # Get article URLs from RSS feed
        logger.info("Fetching Fortune Term Sheet RSS feed...")
        article_urls = await scraper.scrape_rss_feed()
        
        if not article_urls:
            logger.warning("No articles found in RSS feed")
            return {"status": "success", "total_articles": 0, "total_deals": 0}
        
        # Limit number of articles
        article_urls = article_urls[:max_articles]
        logger.info(f"Processing {len(article_urls)} articles from RSS feed")
        
        # Process each article
        all_deals = []
        article_results = []
        
        for i, url in enumerate(article_urls):
            logger.info(f"Processing article {i+1}/{len(article_urls)}: {url}")
            
            try:
                # Extract deals from article
                deals = await scraper.extract_deals_from_article(url)
                
                if deals:
                    all_deals.extend(deals)
                    logger.info(f"Extracted {len(deals)} deals from {url}")
                else:
                    logger.info(f"No deals found in {url}")
                
                article_results.append({
                    "url": url,
                    "status": "success",
                    "deals_found": len(deals)
                })
                
                # Rate limiting
                if i < len(article_urls) - 1:
                    delay = config.scraping.request_delay
                    logger.debug(f"Waiting {delay} seconds before next article...")
                    await asyncio.sleep(delay)
                    
            except Exception as e:
                logger.error(f"Error processing article {url}: {e}")
                article_results.append({
                    "url": url,
                    "status": "error",
                    "error": str(e),
                    "deals_found": 0
                })
        
        # Save deals to database
        saved_count = 0
        if all_deals:
            logger.info(f"Saving {len(all_deals)} deals to database...")
            saved_count = await db_client.upsert_fortune_deals(all_deals)
        
        return {
            "status": "success",
            "total_articles": len(article_urls),
            "articles_processed": len(article_results),
            "articles_successful": sum(1 for r in article_results if r["status"] == "success"),
            "articles_failed": sum(1 for r in article_results if r["status"] == "error"),
            "total_deals": len(all_deals),
            "deals_saved": saved_count,
            "articles": article_results,
            "deals": all_deals
        }
        
    except Exception as e:
        logger.error(f"Error in Fortune RSS scraping: {e}", exc_info=True)
        return {
            "status": "error",
            "error": str(e)
        }
    finally:
        if scraper:
            await scraper.close()
        if db_client:
            await db_client.close()


async def scrape_fortune_articles(
    article_urls: List[str], 
    config: Config, 
    processed_urls_file: str = None
) -> Dict[str, Any]:
    """
    Scrape specific Fortune articles for deals
    
    Args:
        article_urls: List of article URLs to process
        config: Configuration object
        processed_urls_file: File to track processed URLs (optional)
        
    Returns:
        Dictionary with scraping results
    """
    logger = get_logger(__name__)
    scraper = None
    db_client = None
    
    try:
        # Load previously processed URLs
        processed_urls = set()
        if processed_urls_file:
            processed_urls = load_processed_urls(processed_urls_file)
            logger.info(f"Loaded {len(processed_urls)} previously processed URLs")
        
        # Filter out already processed URLs
        new_urls = [url for url in article_urls if url not in processed_urls]
        logger.info(f"Processing {len(new_urls)} new URLs (out of {len(article_urls)} total)")
        
        if not new_urls:
            return {"status": "success", "message": "All URLs already processed"}
        
        # Initialize components
        scraper = FortuneScraper(config.scraping)
        await scraper.start()
        
        db_client = SupabaseClient(config.database)
        
        # Process each article
        all_deals = []
        article_results = []
        successfully_processed = set()
        
        for i, url in enumerate(new_urls):
            logger.info(f"Processing article {i+1}/{len(new_urls)}: {url}")
            
            try:
                # Extract deals from article
                deals = await scraper.extract_deals_from_article(url)
                
                if deals:
                    all_deals.extend(deals)
                    logger.info(f"Extracted {len(deals)} deals from {url}")
                else:
                    logger.info(f"No deals found in {url}")
                
                article_results.append({
                    "url": url,
                    "status": "success",
                    "deals_found": len(deals)
                })
                
                successfully_processed.add(url)
                
                # Rate limiting
                if i < len(new_urls) - 1:
                    delay = config.scraping.request_delay
                    logger.debug(f"Waiting {delay} seconds before next article...")
                    await asyncio.sleep(delay)
                    
            except Exception as e:
                logger.error(f"Error processing article {url}: {e}")
                article_results.append({
                    "url": url,
                    "status": "error",
                    "error": str(e),
                    "deals_found": 0
                })
        
        # Save deals to database
        saved_count = 0
        if all_deals:
            logger.info(f"Saving {len(all_deals)} deals to database...")
            saved_count = await db_client.upsert_fortune_deals(all_deals)
        
        # Update processed URLs file
        if processed_urls_file and successfully_processed:
            processed_urls.update(successfully_processed)
            save_processed_urls(processed_urls_file, processed_urls)
            logger.info(f"Updated processed URLs file with {len(successfully_processed)} new URLs")
        
        return {
            "status": "success",
            "total_articles": len(new_urls),
            "articles_processed": len(article_results),
            "articles_successful": sum(1 for r in article_results if r["status"] == "success"),
            "articles_failed": sum(1 for r in article_results if r["status"] == "error"),
            "total_deals": len(all_deals),
            "deals_saved": saved_count,
            "articles": article_results,
            "deals": all_deals
        }
        
    except Exception as e:
        logger.error(f"Error in article processing: {e}", exc_info=True)
        return {
            "status": "error",
            "error": str(e)
        }
    finally:
        if scraper:
            await scraper.close()
        if db_client:
            await db_client.close()


async def discover_and_scrape_articles(
    config: Config, 
    processed_urls_file: str = "processed_fortune_urls.txt",
    max_articles: int = 20
) -> Dict[str, Any]:
    """
    Discover new Fortune articles and scrape them for deals
    
    Args:
        config: Configuration object
        processed_urls_file: File to track processed URLs
        max_articles: Maximum number of new articles to process
        
    Returns:
        Dictionary with scraping results
    """
    logger = get_logger(__name__)
    scraper = None
    
    try:
        # Initialize scraper
        scraper = FortuneScraper(config.scraping)
        await scraper.start()
        
        # Load previously processed URLs
        processed_urls = load_processed_urls(processed_urls_file)
        logger.info(f"Loaded {len(processed_urls)} previously processed URLs")
        
        # Discover new articles
        logger.info("Discovering new Fortune Term Sheet articles...")
        new_urls = await scraper.discover_new_articles(processed_urls=processed_urls)
        
        if not new_urls:
            logger.info("No new articles found")
            return {"status": "success", "message": "No new articles found"}
        
        # Limit to max_articles
        new_urls = new_urls[:max_articles]
        logger.info(f"Found {len(new_urls)} new articles to process")
        
        # Close scraper before passing to other function
        await scraper.close()
        scraper = None
        
        # Scrape the discovered articles
        result = await scrape_fortune_articles(new_urls, config, processed_urls_file)
        
        return result
        
    except Exception as e:
        logger.error(f"Error in discovery and scraping: {e}", exc_info=True)
        return {
            "status": "error",
            "error": str(e)
        }
    finally:
        if scraper:
            await scraper.close()


def load_urls_from_file(filepath: str) -> List[str]:
    """Load URLs from a text file"""
    if not Path(filepath).exists():
        raise FileNotFoundError(f"URL file not found: {filepath}")
    
    with open(filepath, 'r') as f:
        urls = [line.strip() for line in f if line.strip() and line.startswith('http')]
    
    return urls


def main():
    """Main function with CLI argument parsing"""
    parser = argparse.ArgumentParser(
        description="Fortune Term Sheet Deals Scraper",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Add arguments
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--rss', 
                       action='store_true',
                       help='Scrape latest articles from RSS feed')
    group.add_argument('--discover', 
                       action='store_true',
                       help='Discover and scrape new articles from tag page')
    group.add_argument('--urls', 
                       nargs='+',
                       help='Specific article URLs to scrape')
    group.add_argument('--file', 
                       help='File containing article URLs to scrape')
    
    parser.add_argument('--max-articles', 
                        type=int,
                        default=10,
                        help='Maximum number of articles to process')
    parser.add_argument('--processed-urls-file', 
                        default='processed_fortune_urls.txt',
                        help='File to track processed URLs')
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
        logger.info("Starting Fortune Term Sheet Scraper")
        logger.info(f"Configuration: {args}")
        
        # Run the appropriate operation
        if args.rss:
            logger.info("Scraping from RSS feed")
            result = asyncio.run(scrape_fortune_rss(config, args.max_articles))
        elif args.discover:
            logger.info("Discovering and scraping new articles")
            result = asyncio.run(discover_and_scrape_articles(
                config, 
                args.processed_urls_file, 
                args.max_articles
            ))
        elif args.urls:
            logger.info(f"Scraping {len(args.urls)} specific URLs")
            result = asyncio.run(scrape_fortune_articles(
                args.urls, 
                config, 
                args.processed_urls_file
            ))
        elif args.file:
            logger.info(f"Loading URLs from file: {args.file}")
            urls = load_urls_from_file(args.file)
            logger.info(f"Loaded {len(urls)} URLs from file")
            result = asyncio.run(scrape_fortune_articles(
                urls, 
                config, 
                args.processed_urls_file
            ))
        
        # Print results
        if result["status"] == "success":
            logger.info("✅ Scraping completed successfully")
            
            if "total_articles" in result:
                logger.info(f"Articles processed: {result['total_articles']}")
                if "articles_successful" in result:
                    logger.info(f"Successful: {result['articles_successful']}")
                    logger.info(f"Failed: {result['articles_failed']}")
            
            if "total_deals" in result:
                logger.info(f"Total deals found: {result['total_deals']}")
                logger.info(f"Deals saved: {result.get('deals_saved', 0)}")
            
            if result.get("message"):
                logger.info(result["message"])
                
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
