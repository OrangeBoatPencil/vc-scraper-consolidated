"""
Configuration management for the VC Scraper application.

This module provides centralized configuration management using Pydantic models
for type safety and validation, supporting both environment variables and YAML
configuration files.
"""

import os
import yaml
from pathlib import Path
from typing import Dict, List, Optional, Any, Literal
from pydantic import BaseSettings, BaseModel, Field, validator, root_validator
from pydantic.env_settings import SettingsConfigDict
from enum import Enum


class Environment(str, Enum):
    """Supported environments."""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


class LogLevel(str, Enum):
    """Supported log levels."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class SupabaseSettings(BaseModel):
    """Supabase database configuration."""
    url: str = Field(..., description="Supabase URL")
    anon_key: str = Field(..., description="Supabase anonymous key")
    service_role_key: str = Field(..., description="Supabase service role key")
    pool_size: int = Field(10, description="Connection pool size")
    query_timeout: int = Field(30, description="Query timeout in seconds")
    
    @validator('url')
    def validate_url(cls, v):
        """Validate Supabase URL format."""
        if not v.startswith('https://'):
            raise ValueError('Supabase URL must use HTTPS')
        if not v.endswith('.supabase.co'):
            raise ValueError('Invalid Supabase URL format')
        return v


class ScrapingSettings(BaseModel):
    """Web scraping configuration."""
    max_concurrent_requests: int = Field(5, description="Maximum concurrent requests")
    request_delay: float = Field(1.0, description="Delay between requests in seconds")
    timeout: int = Field(30, description="Request timeout in seconds")
    max_retries: int = Field(3, description="Maximum retry attempts")
    user_agent: str = Field(
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        description="User agent string"
    )
    
    # Browser settings
    headless_browser: bool = Field(True, description="Run browser in headless mode")
    browser_timeout: int = Field(30000, description="Browser timeout in milliseconds")
    
    # Anti-detection measures
    use_random_user_agents: bool = Field(True, description="Rotate user agents")
    enable_stealth_mode: bool = Field(True, description="Enable stealth browsing")
    
    @validator('max_concurrent_requests')
    def validate_concurrent_requests(cls, v):
        """Validate concurrent request limits."""
        if v < 1 or v > 20:
            raise ValueError('max_concurrent_requests must be between 1 and 20')
        return v
    
    @validator('request_delay')
    def validate_request_delay(cls, v):
        """Validate request delay."""
        if v < 0.1:
            raise ValueError('request_delay must be at least 0.1 seconds')
        return v


class SiteConfig(BaseModel):
    """Configuration for a specific VC site."""
    name: str = Field(..., description="Site name")
    url: str = Field(..., description="Main site URL")
    portfolio_url: str = Field(..., description="Portfolio page URL")
    team_url: Optional[str] = Field(None, description="Team page URL")
    description: Optional[str] = Field(None, description="Site description")
    active: bool = Field(True, description="Whether to scrape this site")
    
    # Site-specific overrides
    request_delay: Optional[float] = Field(None, description="Site-specific request delay")
    max_retries: Optional[int] = Field(None, description="Site-specific retry limit")
    use_playwright: bool = Field(False, description="Force browser automation")
    
    @validator('url', 'portfolio_url')
    def validate_urls(cls, v):
        """Validate URL format."""
        if not v.startswith(('http://', 'https://')):
            raise ValueError('URLs must include protocol (http:// or https://)')
        return v


class VCSitesSettings(BaseModel):
    """VC sites configuration."""
    vc_sites: List[SiteConfig] = Field(default_factory=list)
    
    def get_active_sites(self) -> List[SiteConfig]:
        """Get only active sites for scraping."""
        return [site for site in self.vc_sites if site.active]
    
    def get_site_by_name(self, name: str) -> Optional[SiteConfig]:
        """Get a site configuration by name."""
        return next((site for site in self.vc_sites if site.name == name), None)


class DatabaseTableConfig(BaseModel):
    """Database table configuration."""
    enable_change_tracking: bool = Field(True, description="Enable change tracking")
    cleanup_days: int = Field(90, description="Days before cleanup")
    
    @validator('cleanup_days')
    def validate_cleanup_days(cls, v):
        """Validate cleanup days."""
        if v < 1:
            raise ValueError('cleanup_days must be at least 1')
        return v


class DatabaseSettings(BaseModel):
    """Database configuration."""
    connection_pool_size: int = Field(10, description="Connection pool size")
    query_timeout: int = Field(30, description="Query timeout")
    
    # Table configurations
    tables: Dict[str, DatabaseTableConfig] = Field(
        default_factory=lambda: {
            'sites': DatabaseTableConfig(),
            'portfolio_companies': DatabaseTableConfig(),
            'team_members': DatabaseTableConfig(),
            'fortune_deals': DatabaseTableConfig(
                enable_change_tracking=False,
                cleanup_days=180
            )
        }
    )


class LoggingSettings(BaseModel):
    """Logging configuration."""
    level: LogLevel = Field(LogLevel.INFO, description="Log level")
    format: str = Field(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Log format"
    )
    enable_json: bool = Field(True, description="Enable JSON logging")
    enable_sentry: bool = Field(False, description="Enable Sentry error tracking")
    
    # File logging
    file_logging: Dict[str, Any] = Field(
        default_factory=lambda: {
            'enabled': False,
            'path': '/var/log/vc-scraper',
            'max_size': '100MB',
            'backup_count': 5
        }
    )


class FlySettings(BaseModel):
    """Fly.io deployment configuration."""
    region: str = Field("ord", description="Fly.io region")
    app_name: Optional[str] = Field(None, description="Fly.io app name")
    
    # VM configuration
    vm: Dict[str, Any] = Field(
        default_factory=lambda: {
            'memory': '1gb',
            'cpu_kind': 'shared',
            'cpus': 1
        }
    )
    
    # Scaling configuration
    scaling: Dict[str, Any] = Field(
        default_factory=lambda: {
            'min_machines': 0,
            'max_machines': 3,
            'auto_stop_timeout': '10m'
        }
    )


class MonitoringSettings(BaseModel):
    """Monitoring and health check configuration."""
    healthcheck_port: int = Field(8080, description="Health check port")
    healthcheck_path: str = Field("/health", description="Health check endpoint")
    healthcheck_interval: int = Field(15, description="Health check interval")
    healthcheck_timeout: int = Field(5, description="Health check timeout")
    enable_metrics: bool = Field(True, description="Enable metrics collection")
    metrics_port: int = Field(9090, description="Metrics endpoint port")
    
    # Success criteria
    success_criteria: Dict[str, Any] = Field(
        default_factory=lambda: {
            'min_companies_per_site': 5,
            'max_error_rate': 0.1
        }
    )


class LLMSettings(BaseModel):
    """LLM/AI service configuration."""
    openai_api_key: Optional[str] = Field(None, description="OpenAI API key")
    anthropic_api_key: Optional[str] = Field(None, description="Anthropic API key")
    preferred_model: str = Field("gpt-4", description="Preferred LLM model")
    
    # Newsletter extraction settings
    extract_fortune_deals: bool = Field(True, description="Extract deals from Fortune")
    deal_extraction_model: str = Field("gpt-4", description="Model for deal extraction")


# MCP Settings removed - script-based approach


class ApplicationSettings(BaseSettings):
    """Main application settings."""
    
    # Basic settings
    environment: Environment = Field(Environment.DEVELOPMENT, description="Environment")
    debug: bool = Field(False, description="Debug mode")
    
    # Supabase configuration
    supabase_url: str = Field(..., env="SUPABASE_URL")
    supabase_anon_key: str = Field(..., env="SUPABASE_ANON_KEY")
    supabase_service_role_key: str = Field(..., env="SUPABASE_SERVICE_ROLE_KEY")
    
    # Optional API keys
    openai_api_key: Optional[str] = Field(None, env="OPENAI_API_KEY")
    anthropic_api_key: Optional[str] = Field(None, env="ANTHROPIC_API_KEY")
    sentry_dsn: Optional[str] = Field(None, env="SENTRY_DSN")
    firecrawl_api_key: Optional[str] = Field(None, env="FIRECRAWL_API_KEY")
    
    # Proxy settings
    http_proxy: Optional[str] = Field(None, env="HTTP_PROXY")
    https_proxy: Optional[str] = Field(None, env="HTTPS_PROXY")
    
    # Logging
    log_level: LogLevel = Field(LogLevel.INFO, env="LOG_LEVEL")
    
    # Configuration component instances
    _supabase: Optional[SupabaseSettings] = None
    _scraping: Optional[ScrapingSettings] = None
    _sites: Optional[VCSitesSettings] = None
    _database: Optional[DatabaseSettings] = None
    _logging: Optional[LoggingSettings] = None
    _monitoring: Optional[MonitoringSettings] = None
    _fly: Optional[FlySettings] = None
    _llm: Optional[LLMSettings] = None
    
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        case_sensitive=False,
        extra='allow'
    )
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._load_yaml_config()
        self._initialize_components()
    
    def _load_yaml_config(self):
        """Load configuration from YAML files."""
        config_dir = Path(__file__).parent
        
        # Load sites.yaml
        sites_file = config_dir / "sites.yaml"
        if sites_file.exists():
            with open(sites_file, 'r') as f:
                sites_config = yaml.safe_load(f)
                if sites_config:
                    self._sites_config = sites_config
    
    def _initialize_components(self):
        """Initialize configuration components."""
        # Supabase settings
        self._supabase = SupabaseSettings(
            url=self.supabase_url,
            anon_key=self.supabase_anon_key,
            service_role_key=self.supabase_service_role_key
        )
        
        # Load sites configuration
        if hasattr(self, '_sites_config'):
            self._sites = VCSitesSettings(**self._sites_config)
            
            # Apply global scraping config from sites.yaml
            scraping_config = self._sites_config.get('scraping', {})
            self._scraping = ScrapingSettings(**scraping_config)
            
            # Apply database config from sites.yaml
            db_config = self._sites_config.get('database', {})
            self._database = DatabaseSettings(**db_config)
            
            # Apply logging config from sites.yaml
            logging_config = self._sites_config.get('logging', {})
            self._logging = LoggingSettings(**logging_config)
            
            # Apply monitoring config from sites.yaml
            monitoring_config = self._sites_config.get('deployment', {}).get('monitoring', {})
            self._monitoring = MonitoringSettings(**monitoring_config)
            
            # Apply Fly.io config from sites.yaml
            fly_config = self._sites_config.get('deployment', {}).get('fly', {})
            self._fly = FlySettings(**fly_config)
        else:
            # Use defaults if no YAML config
            self._scraping = ScrapingSettings()
            self._sites = VCSitesSettings()
            self._database = DatabaseSettings()
            self._logging = LoggingSettings()
            self._monitoring = MonitoringSettings()
            self._fly = FlySettings()
        
        # Initialize LLM settings
        self._llm = LLMSettings(
            openai_api_key=self.openai_api_key,
            anthropic_api_key=self.anthropic_api_key
        )
    
    @property
    def supabase(self) -> SupabaseSettings:
        """Get Supabase configuration."""
        return self._supabase
    
    @property
    def scraping(self) -> ScrapingSettings:
        """Get scraping configuration."""
        return self._scraping
    
    @property
    def sites(self) -> VCSitesSettings:
        """Get sites configuration."""
        return self._sites
    
    @property
    def database(self) -> DatabaseSettings:
        """Get database configuration."""
        return self._database
    
    @property
    def logging(self) -> LoggingSettings:
        """Get logging configuration."""
        return self._logging
    
    @property
    def monitoring(self) -> MonitoringSettings:
        """Get monitoring configuration."""
        return self._monitoring
    
    @property
    def fly(self) -> FlySettings:
        """Get Fly.io configuration."""
        return self._fly
    
    @property
    def llm(self) -> LLMSettings:
        """Get LLM configuration."""
        return self._llm
    
    def get_scraping_config_for_site(self, domain: str) -> Dict[str, Any]:
        """Get site-specific scraping configuration."""
        base_config = self.scraping.dict()
        
        # Check for site-specific overrides in sites.yaml
        if hasattr(self, '_sites_config'):
            site_configs = self._sites_config.get('scraping', {}).get('site_configs', {})
            if domain in site_configs:
                base_config.update(site_configs[domain])
        
        return base_config
    
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment == Environment.PRODUCTION
    
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.environment == Environment.DEVELOPMENT
    
    def get_database_url(self) -> str:
        """Get the complete database URL for connections."""
        return self.supabase.url
    
    def validate_required_settings(self) -> bool:
        """Validate that all required settings are properly configured."""
        try:
            # Validate Supabase credentials
            assert self.supabase.url, "Supabase URL is required"
            assert self.supabase.service_role_key, "Supabase service role key is required"
            
            # Validate at least one active site
            if not self.sites.get_active_sites():
                raise ValueError("At least one active site must be configured")
            
            return True
        except (AssertionError, ValueError) as e:
            raise ValueError(f"Configuration validation failed: {e}")
    
    def get_health_check_url(self) -> str:
        """Get the health check URL."""
        return f"http://localhost:{self.monitoring.healthcheck_port}{self.monitoring.healthcheck_path}"


# Global settings instance
settings = ApplicationSettings()

# Convenience function for accessing settings
def get_settings() -> ApplicationSettings:
    """Get the application settings instance."""
    return settings


# Additional helper functions
def get_site_config(site_name: str) -> Optional[SiteConfig]:
    """Get configuration for a specific site."""
    return settings.sites.get_site_by_name(site_name)


def get_active_sites() -> List[SiteConfig]:
    """Get all active sites for scraping."""
    return settings.sites.get_active_sites()


def get_supabase_credentials() -> Dict[str, str]:
    """Get Supabase connection credentials."""
    return {
        'url': settings.supabase.url,
        'anon_key': settings.supabase.anon_key,
        'service_role_key': settings.supabase.service_role_key
    }
