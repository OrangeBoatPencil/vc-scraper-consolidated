#!/usr/bin/env python3
"""
Run All Scrapers
Orchestrates all scraping operations: VC portfolios, teams, and Fortune deals
"""
import asyncio
import argparse
import sys
import logging
from pathlib import Path
from typing import Dict, Any
from datetime import datetime

# Add lib to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.utils.logging_config import setup_logging, get_logger
from lib.utils.config import Config
from lib.database.supabase_client import SupabaseClient

# Import the individual scraper functions
from scrape_vc_portfolio import scrape_all_sites as scrape_all_portfolios
from scrape_vc_teams import scrape_all_vc_teams, scrape_teams_from_companies
from scrape_fortune_deals import discover_and_scrape_articles as scrape_fortune


async def run_full_scraping_pipeline(config: Config) -> Dict[str, Any]:
    """
    Run the complete scraping pipeline
    
    Args:
        config: Configuration object
        
    Returns:
        Dictionary with results from all scraping operations
    """
    logger = get_logger(__name__)
    overall_start = datetime.utcnow()
    
    # Initialize results
    results = {
        "started_at": overall_start.isoformat(),
        "completed_at": None,
        "duration_seconds": None,
        "pipeline_status": "running",
        "steps": {}
    }
    
    try:
        # Step 1: Scrape VC portfolios
        logger.info("=" * 50)
        logger.info("STEP 1: Scraping VC Portfolio Companies")
        logger.info("=" * 50)
        
        portfolio_start = datetime.utcnow()
        portfolio_result = await scrape_all_portfolios(config)
        portfolio_duration = (datetime.utcnow() - portfolio_start).total_seconds()
        
        results["steps"]["portfolios"] = {
            "status": portfolio_result["status"],
            "duration_seconds": portfolio_duration,
            "companies_saved": portfolio_result.get("total_companies_saved", 0),
            "sites_successful": portfolio_result.get("sites_successful", 0),
            "sites_failed": portfolio_result.get("sites_failed", 0),
        }
        
        if portfolio_result["status"] != "completed":
            logger.error("Portfolio scraping failed, continuing with other steps...")
        else:
            logger.info(f"✅ Portfolio scraping completed: {portfolio_result['total_companies_saved']} companies saved")
        
        # Step 2: Scrape VC team members
        logger.info("=" * 50)
        logger.info("STEP 2: Scraping VC Team Members")
        logger.info("=" * 50)
        
        vc_teams_start = datetime.utcnow()
        vc_teams_result = await scrape_all_vc_teams(config)
        vc_teams_duration = (datetime.utcnow() - vc_teams_start).total_seconds()
        
        results["steps"]["vc_teams"] = {
            "status": vc_teams_result["status"],
            "duration_seconds": vc_teams_duration,
            "members_saved": vc_teams_result.get("total_members_saved", 0),
            "sites_successful": vc_teams_result.get("vc_sites_successful", 0),
            "sites_failed": vc_teams_result.get("vc_sites_failed", 0),
        }
        
        if vc_teams_result["status"] != "completed":
            logger.error("VC team scraping failed, continuing with other steps...")
        else:
            logger.info(f"✅ VC team scraping completed: {vc_teams_result['total_members_saved']} members saved")
        
        # Step 3: Scrape portfolio company teams (limited to avoid overwhelming)
        logger.info("=" * 50)
        logger.info("STEP 3: Scraping Portfolio Company Teams")
        logger.info("=" * 50)
        
        portfolio_teams_start = datetime.utcnow()
        portfolio_teams_result = await scrape_teams_from_companies(config, max_companies=10)
        portfolio_teams_duration = (datetime.utcnow() - portfolio_teams_start).total_seconds()
        
        results["steps"]["portfolio_teams"] = {
            "status": portfolio_teams_result["status"],
            "duration_seconds": portfolio_teams_duration,
            "members_saved": portfolio_teams_result.get("total_members_saved", 0),
            "teams_found": portfolio_teams_result.get("teams_found", 0),
            "companies_processed": portfolio_teams_result.get("companies_processed", 0),
        }
        
        if portfolio_teams_result["status"] != "completed":
            logger.error("Portfolio team scraping failed, continuing with other steps...")
        else:
            logger.info(f"✅ Portfolio team scraping completed: {portfolio_teams_result['total_members_saved']} members saved")
        
        # Step 4: Scrape Fortune deals
        logger.info("=" * 50)
        logger.info("STEP 4: Scraping Fortune Term Sheet Deals")
        logger.info("=" * 50)
        
        fortune_start = datetime.utcnow()
        fortune_result = await scrape_fortune(config, max_articles=5)
        fortune_duration = (datetime.utcnow() - fortune_start).total_seconds()
        
        results["steps"]["fortune_deals"] = {
            "status": fortune_result["status"],
            "duration_seconds": fortune_duration,
            "deals_saved": fortune_result.get("deals_saved", 0),
            "articles_processed": fortune_result.get("total_articles", 0),
            "articles_successful": fortune_result.get("articles_successful", 0),
        }
        
        if fortune_result["status"] != "success":
            logger.error("Fortune scraping failed, continuing...")
        else:
            logger.info(f"✅ Fortune scraping completed: {fortune_result.get('deals_saved', 0)} deals saved")
        
        # Calculate overall results
        overall_end = datetime.utcnow()
        results["completed_at"] = overall_end.isoformat()
        results["duration_seconds"] = (overall_end - overall_start).total_seconds()
        
        # Determine overall status
        successful_steps = sum(1 for step in results["steps"].values() 
                             if step["status"] in ["success", "completed"])
        total_steps = len(results["steps"])
        
        if successful_steps == total_steps:
            results["pipeline_status"] = "success"
        elif successful_steps > 0:
            results["pipeline_status"] = "partial_success"
        else:
            results["pipeline_status"] = "failed"
        
        # Calculate totals
        results["totals"] = {
            "companies_saved": sum(step.get("companies_saved", 0) for step in results["steps"].values()),
            "members_saved": sum(step.get("members_saved", 0) for step in results["steps"].values()),
            "deals_saved": results["steps"]["fortune_deals"].get("deals_saved", 0),
        }
        
        return results
        
    except Exception as e:
        logger.error(f"Error in scraping pipeline: {e}", exc_info=True)
        
        # Update results with error
        results["pipeline_status"] = "error"
        results["error"] = str(e)
        results["completed_at"] = datetime.utcnow().isoformat()
        results["duration_seconds"] = (datetime.utcnow() - overall_start).total_seconds()
        
        return results


async def run_database_maintenance(config: Config) -> Dict[str, Any]:
    """
    Run database maintenance tasks
    
    Args:
        config: Configuration object
        
    Returns:
        Dictionary with maintenance results
    """
    logger = get_logger(__name__)
    db_client = None
    
    try:
        logger.info("=" * 50)
        logger.info("DATABASE MAINTENANCE")
        logger.info("=" * 50)
        
        db_client = SupabaseClient(config.database)
        
        # Clean up old change records
        logger.info("Cleaning up old change records...")
        cleanup_result = await db_client.cleanup_old_changes(days=30)
        
        # Get database statistics
        logger.info("Gathering database statistics...")
        stats = await db_client.get_statistics()
        
        # Get recent changes
        logger.info("Getting recent changes...")
        recent_company_changes = await db_client.get_recent_changes("portfolio_companies", hours=24)
        recent_member_changes = await db_client.get_recent_changes("team_members", hours=24)
        
        return {
            "status": "success",
            "cleanup_completed": cleanup_result,
            "statistics": stats,
            "recent_changes": {
                "companies": len(recent_company_changes),
                "members": len(recent_member_changes)
            }
        }
        
    except Exception as e:
        logger.error(f"Error in database maintenance: {e}", exc_info=True)
        return {
            "status": "error",
            "error": str(e)
        }
    finally:
        if db_client:
            await db_client.close()


async def health_check(config: Config) -> Dict[str, Any]:
    """
    Perform health check on all components
    
    Args:
        config: Configuration object
        
    Returns:
        Dictionary with health check results
    """
    logger = get_logger(__name__)
    db_client = None
    
    try:
        logger.info("Performing health check...")
        
        # Check database connectivity
        db_client = SupabaseClient(config.database)
        db_healthy = await db_client.ping()
        
        # Check configuration
        config_valid = bool(config.get_vc_sites())
        
        # Check environment variables
        env_valid = bool(config.database.url and config.database.service_role_key)
        
        return {
            "status": "healthy" if all([db_healthy, config_valid, env_valid]) else "unhealthy",
            "components": {
                "database": "healthy" if db_healthy else "unhealthy",
                "configuration": "valid" if config_valid else "invalid",
                "environment": "valid" if env_valid else "invalid"
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in health check: {e}", exc_info=True)
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }
    finally:
        if db_client:
            await db_client.close()


def main():
    """Main function with CLI argument parsing"""
    parser = argparse.ArgumentParser(
        description="Run All VC Scrapers",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Add arguments
    parser.add_argument('--mode', 
                        choices=['full', 'portfolios', 'teams', 'fortune', 'maintenance', 'health'],
                        default='full',
                        help='Scraping mode to run')
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
    parser.add_argument('--output', 
                        help='Output file for results (JSON format)')
    
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
        logger.info("Starting VC Scraper Pipeline")
        logger.info(f"Mode: {args.mode}")
        logger.info(f"Configuration: {args}")
        
        # Run the appropriate operation
        if args.mode == 'full':
            logger.info("Running full scraping pipeline")
            result = asyncio.run(run_full_scraping_pipeline(config))
        elif args.mode == 'portfolios':
            logger.info("Running portfolio scraping only")
            result = asyncio.run(scrape_all_portfolios(config))
        elif args.mode == 'teams':
            logger.info("Running team scraping only")
            vc_result = asyncio.run(scrape_all_vc_teams(config))
            portfolio_result = asyncio.run(scrape_teams_from_companies(config, max_companies=10))
            result = {
                "status": "completed",
                "vc_teams": vc_result,
                "portfolio_teams": portfolio_result
            }
        elif args.mode == 'fortune':
            logger.info("Running Fortune scraping only")
            result = asyncio.run(scrape_fortune(config))
        elif args.mode == 'maintenance':
            logger.info("Running database maintenance")
            result = asyncio.run(run_database_maintenance(config))
        elif args.mode == 'health':
            logger.info("Running health check")
            result = asyncio.run(health_check(config))
        
        # Save results to file if requested
        if args.output:
            import json
            with open(args.output, 'w') as f:
                json.dump(result, f, indent=2, default=str)
            logger.info(f"Results saved to {args.output}")
        
        # Print summary
        if result.get("status") in ["success", "completed"]:
            logger.info("✅ Operation completed successfully")
            
            if args.mode == 'full':
                print("\n" + "=" * 50)
                print("PIPELINE SUMMARY")
                print("=" * 50)
                print(f"Status: {result['pipeline_status']}")
                print(f"Duration: {result['duration_seconds']:.1f} seconds")
                print(f"Companies saved: {result['totals']['companies_saved']}")
                print(f"Team members saved: {result['totals']['members_saved']}")
                print(f"Fortune deals saved: {result['totals']['deals_saved']}")
                print("=" * 50)
        else:
            logger.error("❌ Operation failed")
            logger.error(f"Error: {result.get('error', 'Unknown error')}")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Operation interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
