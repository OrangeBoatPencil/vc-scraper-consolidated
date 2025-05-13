"""
Database migration handler for VC Scraper
Handles running SQL migrations in the correct order
"""
import asyncio
import os
import re
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
import sys

# Add lib to Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from lib.utils.config import Config
from lib.database.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)

class MigrationManager:
    """Manages database migrations"""
    
    def __init__(self, config: Config = None):
        self.config = config or Config()
        self.db_client = SupabaseClient(self.config.database)
        self.migrations_dir = Path(__file__).parent.parent.parent / "migrations"
        
        # Ensure migrations table exists
        self._create_migrations_table()
    
    def _create_migrations_table(self):
        """Create migrations tracking table if it doesn't exist"""
        sql = """
        CREATE TABLE IF NOT EXISTS migrations (
            id SERIAL PRIMARY KEY,
            filename VARCHAR(255) NOT NULL UNIQUE,
            executed_at TIMESTAMP DEFAULT NOW(),
            created_at TIMESTAMP DEFAULT NOW()
        )
        """
        
        try:
            self.db_client.client.rpc('execute_sql', {'query': sql}).execute()
            logger.info("Migrations table ensured")
        except Exception as e:
            logger.error(f"Error creating migrations table: {e}")
            raise
    
    def get_migration_files(self) -> List[Path]:
        """Get all migration files in order"""
        if not self.migrations_dir.exists():
            logger.warning(f"Migrations directory not found: {self.migrations_dir}")
            return []
        
        # Find all .sql files
        sql_files = list(self.migrations_dir.glob("*.sql"))
        
        # Sort by filename (assuming numeric prefix like 001_, 002_, etc.)
        sql_files.sort(key=lambda x: x.name)
        
        return sql_files
    
    def get_executed_migrations(self) -> List[str]:
        """Get list of already executed migrations"""
        try:
            response = self.db_client.client.table('migrations').select('filename').execute()
            return [row['filename'] for row in response.data]
        except Exception as e:
            logger.error(f"Error getting executed migrations: {e}")
            return []
    
    def get_pending_migrations(self) -> List[Path]:
        """Get migrations that haven't been executed yet"""
        all_migrations = self.get_migration_files()
        executed_migrations = set(self.get_executed_migrations())
        
        pending = [
            migration for migration in all_migrations
            if migration.name not in executed_migrations
        ]
        
        return pending
    
    def execute_migration(self, migration_file: Path) -> bool:
        """Execute a single migration file"""
        logger.info(f"Executing migration: {migration_file.name}")
        
        try:
            # Read the migration file
            with open(migration_file, 'r') as f:
                sql_content = f.read()
            
            # Split by semicolons to handle multiple statements
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            # Execute each statement
            for statement in statements:
                if statement:
                    try:
                        self.db_client.client.rpc('execute_sql', {'query': statement}).execute()
                    except Exception as e:
                        logger.error(f"Error executing statement: {statement[:100]}...")
                        raise
            
            # Record the migration as executed
            self.db_client.client.table('migrations').insert({
                'filename': migration_file.name,
                'executed_at': datetime.now(timezone.utc).isoformat()
            }).execute()
            
            logger.info(f"Successfully executed migration: {migration_file.name}")
            return True
            
        except Exception as e:
            logger.error(f"Error executing migration {migration_file.name}: {e}")
            return False
    
    def run_pending_migrations(self) -> Dict[str, Any]:
        """Run all pending migrations"""
        pending_migrations = self.get_pending_migrations()
        
        if not pending_migrations:
            logger.info("No pending migrations found")
            return {
                'status': 'success',
                'message': 'No pending migrations',
                'executed': 0,
                'failed': 0
            }
        
        logger.info(f"Found {len(pending_migrations)} pending migrations")
        
        executed_count = 0
        failed_count = 0
        failed_migrations = []
        
        for migration in pending_migrations:
            success = self.execute_migration(migration)
            if success:
                executed_count += 1
            else:
                failed_count += 1
                failed_migrations.append(migration.name)
        
        result = {
            'status': 'success' if failed_count == 0 else 'partial',
            'message': f"Executed {executed_count} migrations successfully",
            'executed': executed_count,
            'failed': failed_count,
            'failed_migrations': failed_migrations
        }
        
        if failed_count > 0:
            result['message'] += f", {failed_count} failed"
        
        return result
    
    def get_migration_status(self) -> Dict[str, Any]:
        """Get status of all migrations"""
        all_migrations = self.get_migration_files()
        executed_migrations = set(self.get_executed_migrations())
        
        status = {
            'total_migrations': len(all_migrations),
            'executed_migrations': len(executed_migrations),
            'pending_migrations': len(all_migrations) - len(executed_migrations),
            'migrations': []
        }
        
        for migration in all_migrations:
            status['migrations'].append({
                'filename': migration.name,
                'executed': migration.name in executed_migrations,
                'path': str(migration)
            })
        
        return status
    
    async def close(self):
        """Close database connection"""
        if self.db_client:
            await self.db_client.close()

# Standalone script functionality
async def main():
    """Main function for running migrations from command line"""
    import argparse
    from lib.utils.logging_config import setup_logging
    
    parser = argparse.ArgumentParser(description="Database Migration Manager")
    parser.add_argument('command', choices=['status', 'run', 'list'], 
                       help='Command to execute')
    parser.add_argument('--config', help='Path to config file')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be executed without running')
    parser.add_argument('--log-level', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(level=args.log_level)
    
    try:
        # Load configuration
        config = Config(config_file=args.config)
        
        # Create migration manager
        migration_manager = MigrationManager(config)
        
        if args.command == 'status':
            status = migration_manager.get_migration_status()
            print("\nMigration Status:")
            print(f"Total migrations: {status['total_migrations']}")
            print(f"Executed: {status['executed_migrations']}")
            print(f"Pending: {status['pending_migrations']}")
            print("\nDetailed status:")
            for migration in status['migrations']:
                status_icon = "✅" if migration['executed'] else "⏳"
                print(f"{status_icon} {migration['filename']}")
        
        elif args.command == 'list':
            all_migrations = migration_manager.get_migration_files()
            executed_migrations = set(migration_manager.get_executed_migrations())
            
            print("\nAll migrations:")
            for migration in all_migrations:
                status_icon = "✅" if migration.name in executed_migrations else "⏳"
                print(f"{status_icon} {migration.name}")
        
        elif args.command == 'run':
            if args.dry_run:
                pending = migration_manager.get_pending_migrations()
                print(f"\nWould execute {len(pending)} migrations:")
                for migration in pending:
                    print(f"  - {migration.name}")
            else:
                result = migration_manager.run_pending_migrations()
                print(f"\nMigration result: {result['message']}")
                print(f"Status: {result['status']}")
                if result['failed_migrations']:
                    print(f"Failed migrations: {result['failed_migrations']}")
        
        await migration_manager.close()
        
    except Exception as e:
        logger.error(f"Migration error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
