"""
Example usage of the VC Scraper settings configuration.

This file demonstrates how to use the settings module in your application.
"""

import asyncio
from typing import List
from config import settings, get_active_sites, get_site_config


async def main():
    """Example of using settings in an application."""
    
    # 1. Access basic settings
    print(f"Running in {settings.environment} environment")
    print(f"Debug mode: {settings.debug}")
    
    # 2. Get Supabase configuration
    supabase_config = settings.supabase
    print(f"\nSupabase URL: {supabase_config.url}")
    print(f"Connection pool size: {supabase_config.pool_size}")
    
    # 3. Get scraping settings
    scraping_config = settings.scraping
    print(f"\nScraping settings:")
    print(f"  Max concurrent requests: {scraping_config.max_concurrent_requests}")
    print(f"  Request delay: {scraping_config.request_delay} seconds")
    print(f"  Timeout: {scraping_config.timeout} seconds")
    print(f"  Headless browser: {scraping_config.headless_browser}")
    
    # 4. Get active sites to scrape
    active_sites = get_active_sites()
    print(f"\nActive sites ({len(active_sites)}):")
    for site in active_sites:
        print(f"  - {site.name}")
        print(f"    Portfolio URL: {site.portfolio_url}")
        if site.team_url:
            print(f"    Team URL: {site.team_url}")
        
        # Get site-specific configuration
        domain = site.url.split("//")[1].split("/")[0]
        site_config = settings.get_scraping_config_for_site(domain)
        if site_config['request_delay'] != scraping_config.request_delay:
            print(f"    Custom request delay: {site_config['request_delay']} seconds")
    
    # 5. Get MCP server configuration
    mcp_config = settings.mcp
    print(f"\nMCP Configuration:")
    print(f"  Server name: {mcp_config.server_name}")
    print(f"  Version: {mcp_config.server_version}")
    print(f"  VC scraper port: {mcp_config.vc_scraper_port}")
    print(f"  Newsletter scraper port: {mcp_config.newsletter_scraper_port}")
    
    # 6. Check health monitoring settings
    monitoring = settings.monitoring
    print(f"\nMonitoring:")
    print(f"  Health check: {settings.get_health_check_url()}")
    print(f"  Metrics enabled: {monitoring.enable_metrics}")
    print(f"  Metrics port: {monitoring.metrics_port}")
    
    # 7. Validate all settings
    try:
        settings.validate_required_settings()
        print("\n✅ All required settings are properly configured!")
    except ValueError as e:
        print(f"\n❌ Configuration error: {e}")
    
    # 8. Example of using configuration in scraping logic
    print("\nExample scraping workflow:")
    for site in active_sites[:1]:  # Just show first site
        print(f"\nScraping {site.name}...")
        
        # Get site-specific configuration
        domain = site.url.split("//")[1].split("/")[0]
        site_config = settings.get_scraping_config_for_site(domain)
        
        print(f"  Using request delay: {site_config['request_delay']} seconds")
        print(f"  Max retries: {site_config['max_retries']}")
        print(f"  Use Playwright: {site_config.get('use_playwright', False)}")
        
        # Simulate scraping delay
        await asyncio.sleep(0.1)
        print(f"  ✓ Portfolio scraped from {site.portfolio_url}")
        if site.team_url:
            await asyncio.sleep(0.1)
            print(f"  ✓ Team scraped from {site.team_url}")


if __name__ == "__main__":
    asyncio.run(main())
