#!/usr/bin/env python3
"""
Health check endpoint and main application runner for fly.io deployment.

This script serves as the main entry point for the fly.io deployment,
providing health checks and running scheduled scrapers.
"""

import asyncio
import logging
import time
from datetime import datetime
from typing import Dict, Any
import os
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config import settings
from lib.utils.logging_config import setup_logging
from lib.database.supabase_client import SupabaseClient

# Initialize logging
setup_logging()
logger = logging.getLogger(__name__)

class HealthCheck:
    """Health check service for monitoring application status."""
    
    def __init__(self):
        self.start_time = time.time()
        self.last_check = None
        self.status_cache = {}
        
    async def check_database_connection(self) -> Dict[str, Any]:
        """Check Supabase database connectivity."""
        try:
            client = SupabaseClient()
            # Simple query to test connection
            result = client.client.table('sites').select('id').limit(1).execute()
            return {
                'status': 'healthy',
                'message': 'Database connection successful',
                'timestamp': datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return {
                'status': 'unhealthy',
                'message': f'Database connection failed: {str(e)}',
                'timestamp': datetime.utcnow().isoformat()
            }
    
    async def check_configuration(self) -> Dict[str, Any]:
        """Verify configuration is valid."""
        try:
            settings.validate_required_settings()
            return {
                'status': 'healthy',
                'message': 'Configuration is valid',
                'environment': settings.environment,
                'timestamp': datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Configuration check failed: {e}")
            return {
                'status': 'unhealthy',
                'message': f'Configuration invalid: {str(e)}',
                'timestamp': datetime.utcnow().isoformat()
            }
    
    async def run_health_checks(self) -> Dict[str, Any]:
        """Run all health checks and return status."""
        checks = {
            'database': await self.check_database_connection(),
            'configuration': await self.check_configuration()
        }
        
        # Overall status
        all_healthy = all(check['status'] == 'healthy' for check in checks.values())
        
        self.last_check = datetime.utcnow()
        
        return {
            'status': 'healthy' if all_healthy else 'unhealthy',
            'timestamp': self.last_check.isoformat(),
            'uptime_seconds': time.time() - self.start_time,
            'checks': checks,
            'version': '1.0.0'
        }


async def start_http_server():
    """Start HTTP server for health checks."""
    from aiohttp import web
    from aiohttp.web import RouteTableDef
    
    routes = RouteTableDef()
    health_checker = HealthCheck()
    
    @routes.get('/health')
    async def health_check(request):
        """Health check endpoint."""
        checks = await health_checker.run_health_checks()
        status_code = 200 if checks['status'] == 'healthy' else 503
        return web.json_response(checks, status=status_code)
    
    @routes.get('/ping')
    async def ping(request):
        """Simple ping endpoint."""
        return web.json_response({'status': 'ok', 'timestamp': datetime.utcnow().isoformat()})
    
    @routes.get('/status')
    async def status(request):
        """Detailed status endpoint."""
        checks = await health_checker.run_health_checks()
        return web.json_response(checks)
    
    app = web.Application()
    app.add_routes(routes)
    
    # Get port from environment
    port = int(os.getenv('PORT', 8080))
    
    logger.info(f"Starting health check server on port {port}")
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host='0.0.0.0', port=port)
    await site.start()
    
    return runner


async def run_scheduled_scrapers():
    """Run scrapers on a schedule."""
    logger.info("Starting scheduled scraper task")
    
    while True:
        try:
            # Import here to avoid circular imports
            from scripts.run_all_scrapers import main as run_scrapers
            
            logger.info("Running scheduled scrapers...")
            await run_scrapers()
            logger.info("Scheduled scrapers completed")
            
            # Wait 6 hours between runs
            await asyncio.sleep(6 * 60 * 60)
            
        except Exception as e:
            logger.error(f"Error in scheduled scrapers: {e}")
            # Wait 1 hour before retrying on error
            await asyncio.sleep(60 * 60)


async def main():
    """Main application entry point."""
    logger.info("Starting VC Scraper application")
    
    # Set up health check server
    server_runner = await start_http_server()
    
    # Start scheduled scraper task
    scraper_task = asyncio.create_task(run_scheduled_scrapers())
    
    try:
        # Keep the application running
        await asyncio.future()  # Run forever
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    finally:
        # Clean up
        scraper_task.cancel()
        await server_runner.cleanup()


if __name__ == "__main__":
    # Validate settings on startup
    try:
        settings.validate_required_settings()
        logger.info("✅ Configuration validated successfully")
    except ValueError as e:
        logger.error(f"❌ Configuration error: {e}")
        sys.exit(1)
    
    asyncio.run(main())
