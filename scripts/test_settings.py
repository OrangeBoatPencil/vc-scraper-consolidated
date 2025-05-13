#!/usr/bin/env python3
"""
Test script for validating the settings configuration.

This script can be run to verify that all settings are loading correctly
and the configuration is valid.
"""

import sys
import os
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    from config import settings, get_settings, get_active_sites
    
    def test_settings():
        """Test the settings configuration."""
        print("Testing settings configuration...")
        
        # Test basic settings loading
        print(f"Environment: {settings.environment}")
        print(f"Debug mode: {settings.debug}")
        print(f"Log level: {settings.log_level}")
        
        # Test Supabase settings
        print(f"\nSupabase URL: {settings.supabase.url}")
        print(f"Pool size: {settings.supabase.pool_size}")
        
        # Test scraping settings
        print(f"\nMax concurrent requests: {settings.scraping.max_concurrent_requests}")
        print(f"Request delay: {settings.scraping.request_delay}")
        print(f"Headless browser: {settings.scraping.headless_browser}")
        
        # Test site configurations
        active_sites = get_active_sites()
        print(f"\nActive sites: {len(active_sites)}")
        for site in active_sites:
            print(f"  - {site.name}: {site.portfolio_url}")
        
        # Test MCP settings
        print(f"\nMCP server name: {settings.mcp.server_name}")
        print(f"MCP server version: {settings.mcp.server_version}")
        print(f"VC scraper port: {settings.mcp.vc_scraper_port}")
        
        # Validate settings
        try:
            settings.validate_required_settings()
            print("\n✅ Settings validation passed!")
        except ValueError as e:
            print(f"\n❌ Settings validation failed: {e}")
            return False
        
        print("\n✅ All settings loaded successfully!")
        return True
    
    if __name__ == "__main__":
        success = test_settings()
        sys.exit(0 if success else 1)

except ImportError as e:
    print(f"❌ Error importing settings: {e}")
    print("\nMake sure you have installed the required dependencies:")
    print("  pip install pydantic pyyaml")
    sys.exit(1)
