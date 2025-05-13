"""
Enhanced Supabase client with change tracking and advanced features from V1
"""
import asyncio
import json
import hashlib
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timezone
import logging
import aiohttp
import asyncpg
from supabase import create_client, Client
from supabase.lib.client_options import ClientOptions

from ..utils.config import DatabaseConfig
from ..utils.retry_logic import with_retry
from ..cleaning.data_cleaner import DataCleaner

logger = logging.getLogger(__name__)

class SupabaseClient:
    """Enhanced Supabase client with change tracking and advanced features"""
    
    def __init__(self, config: DatabaseConfig = None):
        if config is None:
            config = DatabaseConfig()
        
        self.config = config
        self.client: Client = create_client(
            config.url,
            config.service_role_key,
            options=ClientOptions(
                auto_refresh_token=True,
                persist_session=True,
            )
        )
        self._pool: Optional[asyncpg.Pool] = None
        self.data_cleaner = DataCleaner()
    
    async def _get_connection_pool(self) -> asyncpg.Pool:
        """Get or create asyncpg connection pool for better performance"""
        if self._pool is None:
            # Extract connection details from Supabase URL
            db_url = self.config.url.replace('https://', 'postgresql://postgres:')
            db_url = db_url.replace('.supabase.co', '.pooler.supabase.co:6543')
            db_url += f"?password={self.config.service_role_key.split('.')[0]}"
            
            try:
                self._pool = await asyncpg.create_pool(
                    db_url,
                    min_size=2,
                    max_size=self.config.connection_pool_size,
                    command_timeout=60,
                    server_settings={
                        'application_name': 'vc-scraper',
                    }
                )
                logger.info("Created asyncpg connection pool")
            except Exception as e:
                logger.warning(f"Failed to create asyncpg pool: {e}. Using Supabase client.")
        
        return self._pool
    
    async def ping(self) -> bool:
        """Test database connectivity"""
        try:
            result = self.client.table('sites').select('id').limit(1).execute()
            return True
        except Exception as e:
            logger.error(f"Database ping failed: {e}")
            return False
    
    @with_retry(max_attempts=3, exceptions=(Exception,))
    async def ensure_site_exists(self, name: str, url: str) -> Dict[str, Any]:
        """
        Ensure a site exists in the database, creating it if necessary
        
        Args:
            name: Site name
            url: Site URL
            
        Returns:
            Site record with id
        """
        try:
            # Check if site exists
            response = self.client.table('sites').select('*').eq('url', url).execute()
            
            if response.data:
                site = response.data[0]
                logger.debug(f"Site already exists: {site['name']} ({site['url']})")
                return site
            
            # Create new site
            response = self.client.table('sites').insert({
                'name': name,
                'url': url,
                'created_at': datetime.now(timezone.utc).isoformat(),
            }).execute()
            
            if response.data:
                site = response.data[0]
                logger.info(f"Created new site: {site['name']} ({site['url']})")
                return site
            else:
                raise Exception("Failed to create site")
        
        except Exception as e:
            logger.error(f"Error ensuring site exists: {e}")
            raise
    
    @with_retry(max_attempts=3)
    async def update_site_last_scraped(self, site_id: int) -> bool:
        """Update the last_scraped_at timestamp for a site"""
        try:
            self.client.table('sites').update({
                'last_scraped_at': datetime.now(timezone.utc).isoformat()
            }).eq('id', site_id).execute()
            return True
        except Exception as e:
            logger.error(f"Error updating site last_scraped_at: {e}")
            return False
    
    async def record_page_change(
        self, 
        page_id: int, 
        old_content: str, 
        new_content: str, 
        old_hash: str, 
        new_hash: str
    ) -> bool:
        """Record a page content change"""
        try:
            self.client.table('page_changes').insert({
                'page_id': page_id,
                'previous_content': old_content,
                'new_content': new_content,
                'previous_hash': old_hash,
                'new_hash': new_hash,
                'changed_at': datetime.now(timezone.utc).isoformat()
            }).execute()
            
            logger.info(f"Recorded change for page ID {page_id}")
            return True
        except Exception as e:
            logger.error(f"Error recording page change: {e}")
            return False
    
    async def upsert_portfolio_company(
        self, 
        site_id: int, 
        company_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Upsert portfolio company with change tracking
        
        Args:
            site_id: ID of the site
            company_data: Cleaned company data
            
        Returns:
            Upserted company record
        """
        try:
            # Add site_id to the data
            company_data['site_id'] = site_id
            
            # Check if company already exists
            existing = self.client.table('portfolio_companies')\
                .select('*')\
                .eq('site_id', site_id)\
                .eq('name', company_data['name'])\
                .execute()
            
            if existing.data:
                # Update existing company
                existing_record = existing.data[0]
                old_hash = existing_record.get('content_hash')
                new_hash = company_data.get('content_hash')
                
                # Update timestamps
                company_data['updated_at'] = datetime.now(timezone.utc).isoformat()
                company_data['last_seen_at'] = datetime.now(timezone.utc).isoformat()
                
                response = self.client.table('portfolio_companies')\
                    .update(company_data)\
                    .eq('id', existing_record['id'])\
                    .execute()
                
                if response.data:
                    updated_record = response.data[0]
                    
                    # Record change if content changed
                    if old_hash and new_hash and old_hash != new_hash:
                        await self.record_company_change(
                            updated_record['id'],
                            existing_record,
                            updated_record
                        )
                    
                    logger.debug(f"Updated company: {company_data['name']}")
                    return updated_record
            else:
                # Insert new company
                company_data['created_at'] = datetime.now(timezone.utc).isoformat()
                company_data['first_seen_at'] = datetime.now(timezone.utc).isoformat()
                company_data['last_seen_at'] = datetime.now(timezone.utc).isoformat()
                
                response = self.client.table('portfolio_companies')\
                    .insert(company_data)\
                    .execute()
                
                if response.data:
                    new_record = response.data[0]
                    logger.info(f"Created new company: {company_data['name']}")
                    return new_record
            
            return None
        
        except Exception as e:
            logger.error(f"Error upserting portfolio company: {e}")
            raise
    
    async def record_company_change(
        self, 
        company_id: int, 
        old_data: Dict[str, Any], 
        new_data: Dict[str, Any]
    ) -> bool:
        """Record a company data change"""
        try:
            # Calculate what changed
            changes = {}
            for key, new_value in new_data.items():
                old_value = old_data.get(key)
                if old_value != new_value:
                    changes[key] = {
                        'old': old_value,
                        'new': new_value
                    }
            
            if changes:
                self.client.table('company_changes').insert({
                    'company_id': company_id,
                    'changes': json.dumps(changes),
                    'previous_hash': old_data.get('content_hash'),
                    'new_hash': new_data.get('content_hash'),
                    'changed_at': datetime.now(timezone.utc).isoformat()
                }).execute()
                
                logger.info(f"Recorded changes for company ID {company_id}: {list(changes.keys())}")
            
            return True
        except Exception as e:
            logger.error(f"Error recording company change: {e}")
            return False
    
    async def upsert_team_member(
        self, 
        site_id: int, 
        member_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Upsert team member with change tracking
        
        Args:
            site_id: ID of the site
            member_data: Cleaned member data
            
        Returns:
            Upserted member record
        """
        try:
            # Add site_id to the data
            member_data['site_id'] = site_id
            
            # Check if member already exists
            existing = self.client.table('team_members')\
                .select('*')\
                .eq('site_id', site_id)\
                .eq('name', member_data['name'])\
                .execute()
            
            if existing.data:
                # Update existing member
                existing_record = existing.data[0]
                old_hash = existing_record.get('content_hash')
                new_hash = member_data.get('content_hash')
                
                # Update timestamps
                member_data['updated_at'] = datetime.now(timezone.utc).isoformat()
                member_data['last_seen_at'] = datetime.now(timezone.utc).isoformat()
                
                response = self.client.table('team_members')\
                    .update(member_data)\
                    .eq('id', existing_record['id'])\
                    .execute()
                
                if response.data:
                    updated_record = response.data[0]
                    
                    # Record change if content changed
                    if old_hash and new_hash and old_hash != new_hash:
                        await self.record_member_change(
                            updated_record['id'],
                            existing_record,
                            updated_record
                        )
                    
                    logger.debug(f"Updated team member: {member_data['name']}")
                    return updated_record
            else:
                # Insert new member
                member_data['created_at'] = datetime.now(timezone.utc).isoformat()
                member_data['first_seen_at'] = datetime.now(timezone.utc).isoformat()
                member_data['last_seen_at'] = datetime.now(timezone.utc).isoformat()
                
                response = self.client.table('team_members')\
                    .insert(member_data)\
                    .execute()
                
                if response.data:
                    new_record = response.data[0]
                    logger.info(f"Created new team member: {member_data['name']}")
                    return new_record
            
            return None
        
        except Exception as e:
            logger.error(f"Error upserting team member: {e}")
            raise
    
    async def record_member_change(
        self, 
        member_id: int, 
        old_data: Dict[str, Any], 
        new_data: Dict[str, Any]
    ) -> bool:
        """Record a team member data change"""
        try:
            # Calculate what changed
            changes = {}
            for key, new_value in new_data.items():
                old_value = old_data.get(key)
                if old_value != new_value:
                    changes[key] = {
                        'old': old_value,
                        'new': new_value
                    }
            
            if changes:
                self.client.table('member_changes').insert({
                    'member_id': member_id,
                    'changes': json.dumps(changes),
                    'previous_hash': old_data.get('content_hash'),
                    'new_hash': new_data.get('content_hash'),
                    'changed_at': datetime.now(timezone.utc).isoformat()
                }).execute()
                
                logger.info(f"Recorded changes for member ID {member_id}: {list(changes.keys())}")
            
            return True
        except Exception as e:
            logger.error(f"Error recording member change: {e}")
            return False
    
    async def upsert_companies_with_change_tracking(
        self, 
        site_id: int, 
        companies: List[Dict[str, Any]]
    ) -> int:
        """
        Upsert multiple companies with optimized batch processing
        
        Args:
            site_id: Site ID
            companies: List of cleaned company data
            
        Returns:
            Number of successfully processed companies
        """
        success_count = 0
        
        # Process in batches for better performance
        batch_size = 50
        for i in range(0, len(companies), batch_size):
            batch = companies[i:i + batch_size]
            
            # Process batch
            tasks = [
                self.upsert_portfolio_company(site_id, company)
                for company in batch
            ]
            
            try:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for j, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.error(f"Error processing company {i+j}: {result}")
                    elif result:
                        success_count += 1
                    
            except Exception as e:
                logger.error(f"Error processing batch {i//batch_size + 1}: {e}")
        
        logger.info(f"Successfully processed {success_count}/{len(companies)} companies")
        return success_count
    
    async def upsert_team_members_with_change_tracking(
        self, 
        site_id: int, 
        members: List[Dict[str, Any]]
    ) -> int:
        """
        Upsert multiple team members with optimized batch processing
        
        Args:
            site_id: Site ID
            members: List of cleaned member data
            
        Returns:
            Number of successfully processed members
        """
        success_count = 0
        
        # Process in batches for better performance
        batch_size = 50
        for i in range(0, len(members), batch_size):
            batch = members[i:i + batch_size]
            
            # Process batch
            tasks = [
                self.upsert_team_member(site_id, member)
                for member in batch
            ]
            
            try:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for j, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.error(f"Error processing member {i+j}: {result}")
                    elif result:
                        success_count += 1
                    
            except Exception as e:
                logger.error(f"Error processing batch {i//batch_size + 1}: {e}")
        
        logger.info(f"Successfully processed {success_count}/{len(members)} team members")
        return success_count
    
    async def upsert_fortune_deals(self, deals: List[Dict[str, Any]]) -> int:
        """
        Upsert Fortune deals with deduplication
        
        Args:
            deals: List of cleaned deal data
            
        Returns:
            Number of successfully processed deals
        """
        success_count = 0
        
        for deal in deals:
            try:
                # Check if deal already exists by source article URL
                existing = self.client.table('fortune_deals')\
                    .select('*')\
                    .eq('source_article_url', deal['source_article_url'])\
                    .eq('startup_name', deal['startup_name'])\
                    .execute()
                
                if existing.data:
                    # Update existing deal
                    response = self.client.table('fortune_deals')\
                        .update({
                            **deal,
                            'updated_at': datetime.now(timezone.utc).isoformat()
                        })\
                        .eq('id', existing.data[0]['id'])\
                        .execute()
                    
                    if response.data:
                        logger.debug(f"Updated Fortune deal: {deal['startup_name']}")
                        success_count += 1
                else:
                    # Insert new deal
                    deal['created_at'] = datetime.now(timezone.utc).isoformat()
                    
                    response = self.client.table('fortune_deals')\
                        .insert(deal)\
                        .execute()
                    
                    if response.data:
                        logger.info(f"Created new Fortune deal: {deal['startup_name']}")
                        success_count += 1
            
            except Exception as e:
                logger.error(f"Error processing Fortune deal {deal.get('startup_name', 'Unknown')}: {e}")
        
        logger.info(f"Successfully processed {success_count}/{len(deals)} Fortune deals")
        return success_count
    
    async def get_recent_changes(
        self, 
        table: str, 
        hours: int = 24, 
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get recent changes for a specific table"""
        cutoff_time = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        cutoff_time = cutoff_time.replace(hour=cutoff_time.hour - hours)
        
        try:
            change_table = f"{table}_changes"
            response = self.client.table(change_table)\
                .select('*')\
                .gte('changed_at', cutoff_time.isoformat())\
                .order('changed_at', desc=True)\
                .limit(limit)\
                .execute()
            
            return response.data
        except Exception as e:
            logger.error(f"Error getting recent changes for {table}: {e}")
            return []
    
    async def cleanup_old_changes(self, days: int = 30) -> bool:
        """Clean up old change records to prevent table bloat"""
        cutoff_time = datetime.now(timezone.utc)
        cutoff_time = cutoff_time.replace(day=cutoff_time.day - days)
        
        try:
            # Clean up different change tables
            tables = ['company_changes', 'member_changes', 'page_changes']
            
            for table in tables:
                try:
                    self.client.table(table)\
                        .delete()\
                        .lt('changed_at', cutoff_time.isoformat())\
                        .execute()
                    
                    logger.info(f"Cleaned up old changes from {table}")
                except Exception as e:
                    logger.error(f"Error cleaning up {table}: {e}")
            
            return True
        except Exception as e:
            logger.error(f"Error during change cleanup: {e}")
            return False
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get database statistics"""
        stats = {}
        
        try:
            # Sites statistics
            sites_response = self.client.table('sites').select('*', count='exact').execute()
            stats['total_sites'] = len(sites_response.data)
            
            # Companies statistics
            companies_response = self.client.table('portfolio_companies').select('*', count='exact').execute()
            stats['total_companies'] = len(companies_response.data)
            
            # Team members statistics
            members_response = self.client.table('team_members').select('*', count='exact').execute()
            stats['total_team_members'] = len(members_response.data)
            
            # Fortune deals statistics
            deals_response = self.client.table('fortune_deals').select('*', count='exact').execute()
            stats['total_fortune_deals'] = len(deals_response.data)
            
            # Recent activity (last 24 hours)
            recent_cutoff = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            recent_cutoff = recent_cutoff.replace(hour=recent_cutoff.hour - 24)
            
            recent_companies = self.client.table('portfolio_companies')\
                .select('*', count='exact')\
                .gte('updated_at', recent_cutoff.isoformat())\
                .execute()
            stats['companies_updated_24h'] = len(recent_companies.data)
            
            recent_members = self.client.table('team_members')\
                .select('*', count='exact')\
                .gte('updated_at', recent_cutoff.isoformat())\
                .execute()
            stats['members_updated_24h'] = len(recent_members.data)
            
            return stats
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            return {}
    
    async def close(self):
        """Close database connections"""
        if self._pool:
            await self._pool.close()
