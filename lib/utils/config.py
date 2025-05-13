"""
Configuration management for VC Scraper
"""
import os
import yaml
from typing import Dict, List, Any, Optional
from pathlib import Path
from dataclasses import dataclass, field
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

@dataclass
class ScrapingConfig:
    """Configuration for scraping behavior"""
    max_retries: int = 3
    request_delay: float = 1.0
    timeout: int = 30
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    max_concurrent_requests: int = 5

@dataclass
class DatabaseConfig:
    """Database configuration"""
    url: str = field(default_factory=lambda: os.getenv("SUPABASE_URL", ""))
    service_role_key: str = field(default_factory=lambda: os.getenv("SUPABASE_SERVICE_ROLE_KEY", ""))
    anon_key: str = field(default_factory=lambda: os.getenv("SUPABASE_ANON_KEY", ""))
    connection_pool_size: int = 10
    
    def __post_init__(self):
        if not self.url or not self.service_role_key:
            raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set")

@dataclass
class LoggingConfig:
    """Logging configuration"""
    level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file_path: Optional[str] = None
    enable_sentry: bool = field(default_factory=lambda: bool(os.getenv("SENTRY_DSN")))
    sentry_dsn: Optional[str] = field(default_factory=lambda: os.getenv("SENTRY_DSN"))

@dataclass
class VCSite:
    """Configuration for a VC firm site"""
    name: str
    url: str
    portfolio_url: str
    team_url: Optional[str] = None
    logo_url: Optional[str] = None
    description: Optional[str] = None
    active: bool = True

class Config:
    """Main configuration class"""
    
    def __init__(self, config_file: Optional[str] = None):
        self.config_file = config_file or self._find_config_file()
        self._load_config()
        
        # Initialize sub-configurations
        self.scraping = ScrapingConfig()
        self.database = DatabaseConfig()
        self.logging = LoggingConfig()
        
    def _find_config_file(self) -> str:
        """Find configuration file in standard locations"""
        possible_paths = [
            "config/settings.yaml",
            "settings.yaml",
            os.path.expanduser("~/.vc-scraper/settings.yaml"),
            "/etc/vc-scraper/settings.yaml"
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                return path
        
        # Create default config if none found
        return self._create_default_config()
    
    def _create_default_config(self) -> str:
        """Create a default configuration file"""
        config_dir = "config"
        os.makedirs(config_dir, exist_ok=True)
        
        default_config = {
            "vc_sites": [
                {
                    "name": "Example VC",
                    "url": "https://example-vc.com",
                    "portfolio_url": "https://example-vc.com/portfolio",
                    "team_url": "https://example-vc.com/team",
                    "active": False
                }
            ],
            "scraping": {
                "max_retries": 3,
                "request_delay": 1.0,
                "timeout": 30
            },
            "database": {
                "connection_pool_size": 10
            },
            "logging": {
                "level": "INFO",
                "enable_sentry": False
            }
        }
        
        config_path = os.path.join(config_dir, "settings.yaml")
        with open(config_path, 'w') as f:
            yaml.dump(default_config, f, default_flow_style=False, indent=2)
        
        return config_path
    
    def _load_config(self):
        """Load configuration from file"""
        try:
            with open(self.config_file, 'r') as f:
                self._config_data = yaml.safe_load(f) or {}
        except FileNotFoundError:
            self._config_data = {}
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in config file {self.config_file}: {e}")
    
    def get_vc_sites(self) -> List[VCSite]:
        """Get list of VC sites to scrape"""
        sites_data = self._config_data.get("vc_sites", [])
        sites = []
        
        for site_data in sites_data:
            if site_data.get("active", True):
                sites.append(VCSite(**site_data))
        
        return sites
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key"""
        keys = key.split('.')
        value = self._config_data
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def get_environment(self) -> str:
        """Get current environment"""
        return os.getenv("ENVIRONMENT", "development")
    
    def is_production(self) -> bool:
        """Check if running in production"""
        return self.get_environment() == "production"
    
    def get_sentry_config(self) -> Dict[str, Any]:
        """Get Sentry configuration for error tracking"""
        if not self.logging.enable_sentry or not self.logging.sentry_dsn:
            return {}
        
        return {
            "dsn": self.logging.sentry_dsn,
            "environment": self.get_environment(),
            "traces_sample_rate": 0.1 if self.is_production() else 1.0,
        }
