#!/usr/bin/env python3
"""
Health Check Server for VC Scraper
Provides health endpoints for Fly.io monitoring and system status
"""
import asyncio
import sys
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any
import aiohttp
from aiohttp import web
import json

# Add lib to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.utils.logging_config import setup_logging, get_logger
from lib.utils.config import Config
from lib.database.supabase_client import SupabaseClient


class HealthCheckServer:
    """Health check server for monitoring system status"""
    
    def __init__(self, config: Config):
        self.config = config
        self.db_client = SupabaseClient(config.database)
        self.app = web.Application()
        self.setup_routes()
        
        self.start_time = datetime.now(timezone.utc)
        self.health_checks = {
            'database': False,
            'last_check': None,
            'check_count': 0
        }
    
    def setup_routes(self):
        """Setup health check routes"""
        self.app.router.add_get('/', self.index)
        self.app.router.add_get('/health', self.health_check)
        self.app.router.add_get('/health/simple', self.simple_health)
        self.app.router.add_get('/health/detailed', self.detailed_health)
        self.app.router.add_get('/status', self.system_status)
        self.app.router.add_get('/metrics', self.metrics)
    
    async def index(self, request):
        """Root endpoint with basic info"""
        return web.json_response({
            'service': 'vc-scraper',
            'version': '1.0.0',
            'status': 'running',
            'uptime_seconds': (datetime.now(timezone.utc) - self.start_time).total_seconds(),
            'endpoints': [
                '/health',
                '/health/simple',
                '/health/detailed',
                '/status',
                '/metrics'
            ]
        })
    
    async def simple_health(self, request):
        """Simple health check for Fly.io"""
        try:
            # Basic database ping
            is_healthy = await self.db_client.ping()
            
            if is_healthy:
                return web.json_response({'status': 'ok'}, status=200)
            else:
                return web.json_response({'status': 'error'}, status=503)
                
        except Exception as e:
            logger = get_logger(__name__)
            logger.error(f"Health check failed: {e}")
            return web.json_response({'status': 'error', 'error': str(e)}, status=503)
    
    async def health_check(self, request):
        """Main health check endpoint"""
        try:
            # Perform multiple health checks
            checks = await self.perform_health_checks()
            
            # Determine overall health
            all_healthy = all(checks.values())
            status_code = 200 if all_healthy else 503
            
            return web.json_response({
                'status': 'healthy' if all_healthy else 'unhealthy',
                'checks': checks,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }, status=status_code)
            
        except Exception as e:
            logger = get_logger(__name__)
            logger.error(f"Health check failed: {e}")
            return web.json_response({
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }, status=503)
    
    async def detailed_health(self, request):
        """Detailed health check with additional metrics"""
        try:
            # Basic health checks
            checks = await self.perform_health_checks()
            
            # Get database statistics
            stats = await self.db_client.get_statistics() if checks.get('database') else {}
            
            # Get recent activity
            recent_activity = {}
            if checks.get('database'):
                try:
                    # Check for recent scraping activity
                    recent_companies = await self.db_client.get_recent_changes('portfolio_companies', hours=24)
                    recent_members = await self.db_client.get_recent_changes('team_members', hours=24)
                    
                    recent_activity = {
                        'companies_changed_24h': len(recent_companies),
                        'members_changed_24h': len(recent_members),
                        'last_company_change': recent_companies[0]['changed_at'] if recent_companies else None,
                        'last_member_change': recent_members[0]['changed_at'] if recent_members else None
                    }
                except Exception as e:
                    logger = get_logger(__name__)
                    logger.warning(f"Error getting recent activity: {e}")
            
            # Determine overall health
            all_healthy = all(checks.values())
            
            return web.json_response({
                'status': 'healthy' if all_healthy else 'unhealthy',
                'checks': checks,
                'statistics': stats,
                'recent_activity': recent_activity,
                'uptime_seconds': (datetime.now(timezone.utc) - self.start_time).total_seconds(),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }, status=200 if all_healthy else 503)
            
        except Exception as e:
            logger = get_logger(__name__)
            logger.error(f"Detailed health check failed: {e}")
            return web.json_response({
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }, status=503)
    
    async def system_status(self, request):
        """System status endpoint with operational information"""
        try:
            # Get environment info
            status = {
                'environment': self.config.environment,
                'version': '1.0.0',
                'start_time': self.start_time.isoformat(),
                'uptime_seconds': (datetime.now(timezone.utc) - self.start_time).total_seconds(),
                'config': {
                    'max_concurrent_requests': getattr(self.config.scraping, 'max_concurrent_requests', 'unknown'),
                    'request_delay': getattr(self.config.scraping, 'request_delay', 'unknown'),
                    'database_pool_size': getattr(self.config.database, 'connection_pool_size', 'unknown')
                }
            }
            
            # Add database stats if available
            if await self.db_client.ping():
                stats = await self.db_client.get_statistics()
                status['database_stats'] = stats
            
            return web.json_response(status)
            
        except Exception as e:
            logger = get_logger(__name__)
            logger.error(f"System status failed: {e}")
            return web.json_response({
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }, status=500)
    
    async def metrics(self, request):
        """Prometheus-style metrics endpoint"""
        try:
            # Basic metrics
            uptime = (datetime.now(timezone.utc) - self.start_time).total_seconds()
            
            metrics = [
                f"# HELP vc_scraper_uptime_seconds Application uptime in seconds",
                f"# TYPE vc_scraper_uptime_seconds gauge",
                f"vc_scraper_uptime_seconds {uptime}",
                "",
                f"# HELP vc_scraper_health_checks_total Total number of health checks performed",
                f"# TYPE vc_scraper_health_checks_total counter",
                f"vc_scraper_health_checks_total {self.health_checks['check_count']}",
                "",
                f"# HELP vc_scraper_database_status Database connectivity status (1=connected, 0=disconnected)",
                f"# TYPE vc_scraper_database_status gauge",
                f"vc_scraper_database_status {1 if self.health_checks['database'] else 0}",
            ]
            
            # Add database metrics if available
            if self.health_checks['database']:
                try:
                    stats = await self.db_client.get_statistics()
                    
                    metrics.extend([
                        "",
                        f"# HELP vc_scraper_total_sites Total number of VC sites in database",
                        f"# TYPE vc_scraper_total_sites gauge",
                        f"vc_scraper_total_sites {stats.get('total_sites', 0)}",
                        "",
                        f"# HELP vc_scraper_total_companies Total number of portfolio companies",
                        f"# TYPE vc_scraper_total_companies gauge",
                        f"vc_scraper_total_companies {stats.get('total_companies', 0)}",
                        "",
                        f"# HELP vc_scraper_total_team_members Total number of team members",
                        f"# TYPE vc_scraper_total_team_members gauge",
                        f"vc_scraper_total_team_members {stats.get('total_team_members', 0)}",
                        "",
                        f"# HELP vc_scraper_companies_updated_24h Companies updated in last 24 hours",
                        f"# TYPE vc_scraper_companies_updated_24h gauge",
                        f"vc_scraper_companies_updated_24h {stats.get('companies_updated_24h', 0)}",
                    ])
                except Exception as e:
                    logger = get_logger(__name__)
                    logger.warning(f"Error getting metrics stats: {e}")
            
            return web.Response(text="\n".join(metrics), content_type='text/plain')
            
        except Exception as e:
            logger = get_logger(__name__)
            logger.error(f"Metrics endpoint failed: {e}")
            return web.Response(text=f"Error: {e}", status=500)
    
    async def perform_health_checks(self) -> Dict[str, bool]:
        """Perform all health checks"""
        checks = {}
        
        # Database health check
        try:
            checks['database'] = await self.db_client.ping()
        except Exception as e:
            logger = get_logger(__name__)
            logger.error(f"Database health check failed: {e}")
            checks['database'] = False
        
        # Update health check metadata
        self.health_checks['last_check'] = datetime.now(timezone.utc).isoformat()
        self.health_checks['check_count'] += 1
        self.health_checks['database'] = checks['database']
        
        return checks
    
    async def run(self, host='0.0.0.0', port=8080):
        """Run the health check server"""
        logger = get_logger(__name__)
        logger.info(f"Starting health check server on {host}:{port}")
        
        runner = web.AppRunner(self.app)
        await runner.setup()
        
        site = web.TCPSite(runner, host, port)
        await site.start()
        
        logger.info("Health check server started successfully")
        
        # Keep the server running
        try:
            await asyncio.Future()  # Run forever
        except KeyboardInterrupt:
            logger.info("Shutting down health check server")
        finally:
            await runner.cleanup()


async def main():
    """Main function"""
    # Setup logging
    setup_logging(level='INFO')
    logger = get_logger(__name__)
    
    try:
        # Load configuration
        config = Config()
        
        # Create and run health check server
        server = HealthCheckServer(config)
        await server.run()
        
    except Exception as e:
        logger.error(f"Failed to start health check server: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
