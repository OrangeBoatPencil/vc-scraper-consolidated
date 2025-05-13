"""
Centralized settings configuration for VC Scraper
Provides additional settings beyond the basic config.py
"""
import os
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from pathlib import Path

@dataclass
class ScraperSettings:
    """Settings for individual scrapers"""
    # Retry and resilience settings
    max_retry_attempts: int = 3
    retry_backoff_factor: float = 2.0
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_timeout: float = 300.0  # 5 minutes
    
    # Rate limiting
    base_request_delay: float = 1.0
    max_concurrent_requests: int = 3
    
    # Timeouts
    page_load_timeout: int = 30
    element_wait_timeout: int = 10
    
    # User agent rotation
    use_random_user_agents: bool = True
    custom_user_agents: List[str] = field(default_factory=lambda: [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ])
    
    # Browser settings
    headless_browser: bool = True
    browser_viewport: Dict[str, int] = field(default_factory=lambda: {"width": 1920, "height": 1080})
    
    # Proxy settings
    use_proxy_rotation: bool = False
    proxy_list: List[str] = field(default_factory=list)
    proxy_timeout: int = 30

@dataclass
class DatabaseSettings:
    """Database-related settings"""
    # Connection pooling
    connection_pool_size: int = 10
    connection_pool_max_overflow: int = 5
    connection_pool_timeout: int = 30
    
    # Query settings
    query_timeout: int = 60
    batch_size: int = 100
    
    # Change tracking
    enable_change_tracking: bool = True
    change_retention_days: int = 30
    
    # Cleanup settings
    enable_automatic_cleanup: bool = True
    cleanup_interval_hours: int = 24

@dataclass
class ValidationSettings:
    """Data validation settings"""
    # Company validation
    min_company_name_length: int = 2
    max_company_name_length: int = 100
    required_company_fields: List[str] = field(default_factory=lambda: ["name"])
    
    # Team member validation
    min_member_name_length: int = 2
    max_member_name_length: int = 100
    required_member_fields: List[str] = field(default_factory=lambda: ["name"])
    
    # URL validation
    validate_urls: bool = True
    url_timeout: int = 10
    
    # Data quality thresholds
    min_data_quality_score: float = 0.7
    enable_data_quality_logging: bool = True

@dataclass
class MonitoringSettings:
    """Monitoring and observability settings"""
    # Health checks
    health_check_interval: int = 15  # seconds
    health_check_timeout: int = 5
    
    # Metrics
    enable_metrics: bool = True
    metrics_port: int = 9090
    
    # Alerting
    enable_alerting: bool = False
    alert_thresholds: Dict[str, Any] = field(default_factory=lambda: {
        "error_rate": 0.1,  # 10% error rate
        "response_time": 5.0,  # 5 seconds
        "circuit_breaker_open": True
    })
    
    # Logging
    log_level: str = "INFO"
    log_format: str = "json"
    log_retention_days: int = 7
    enable_structured_logging: bool = True

@dataclass
class SecuritySettings:
    """Security-related settings"""
    # Request headers
    default_headers: Dict[str, str] = field(default_factory=lambda: {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    })
    
    # SSL/TLS settings
    verify_ssl: bool = True
    ssl_timeout: int = 30
    
    # Rate limiting protection
    rate_limit_window: int = 60  # seconds
    max_requests_per_window: int = 100
    
    # IP rotation
    enable_ip_rotation: bool = False
    ip_rotation_interval: int = 300  # 5 minutes

@dataclass
class PerformanceSettings:
    """Performance optimization settings"""
    # Caching
    enable_response_caching: bool = True
    cache_ttl: int = 3600  # 1 hour
    cache_max_size: int = 1000
    
    # Compression
    enable_compression: bool = True
    compression_threshold: int = 1024  # bytes
    
    # Resource optimization
    disable_images: bool = False
    disable_javascript: bool = False
    disable_css: bool = False
    
    # Memory management
    max_memory_usage: int = 2048  # MB
    garbage_collection_threshold: int = 1024  # MB

class AppSettings:
    """Main application settings class"""
    
    def __init__(self, environment: str = None):
        self.environment = environment or os.getenv("ENVIRONMENT", "development")
        self.scraper = ScraperSettings()
        self.database = DatabaseSettings()
        self.validation = ValidationSettings()
        self.monitoring = MonitoringSettings()
        self.security = SecuritySettings()
        self.performance = PerformanceSettings()
        
        # Load environment-specific settings
        self._load_environment_settings()
    
    def _load_environment_settings(self):
        """Load settings based on environment"""
        if self.environment == "production":
            self._apply_production_settings()
        elif self.environment == "development":
            self._apply_development_settings()
        elif self.environment == "testing":
            self._apply_testing_settings()
    
    def _apply_production_settings(self):
        """Apply production-specific settings"""
        # Stricter settings for production
        self.scraper.circuit_breaker_failure_threshold = 3
        self.scraper.max_retry_attempts = 2
        self.scraper.base_request_delay = 2.0
        
        # Enhanced monitoring
        self.monitoring.enable_metrics = True
        self.monitoring.enable_alerting = True
        self.monitoring.log_level = "WARNING"
        
        # Security hardening
        self.security.verify_ssl = True
        self.security.rate_limit_window = 60
        self.security.max_requests_per_window = 50
        
        # Performance optimization
        self.performance.enable_response_caching = True
        self.performance.enable_compression = True
    
    def _apply_development_settings(self):
        """Apply development-specific settings"""
        # More lenient settings for development
        self.scraper.circuit_breaker_failure_threshold = 10
        self.scraper.max_retry_attempts = 5
        self.scraper.base_request_delay = 0.5
        
        # Detailed logging
        self.monitoring.log_level = "DEBUG"
        self.monitoring.enable_structured_logging = True
        
        # Allow self-signed certificates
        self.security.verify_ssl = False
        
        # Disable performance optimizations for easier debugging
        self.performance.enable_response_caching = False
        self.performance.disable_javascript = False
    
    def _apply_testing_settings(self):
        """Apply testing-specific settings"""
        # Fast and deterministic settings for testing
        self.scraper.circuit_breaker_failure_threshold = 5
        self.scraper.max_retry_attempts = 1
        self.scraper.base_request_delay = 0.1
        
        # Minimal logging
        self.monitoring.log_level = "ERROR"
        self.monitoring.enable_metrics = False
        
        # Disable external dependencies
        self.performance.enable_response_caching = False
        self.scraper.use_proxy_rotation = False
    
    def get_scraper_settings(self, scraper_type: str = "default") -> Dict[str, Any]:
        """Get settings for a specific scraper type"""
        base_settings = {
            "max_retry_attempts": self.scraper.max_retry_attempts,
            "retry_backoff_factor": self.scraper.retry_backoff_factor,
            "circuit_breaker_failure_threshold": self.scraper.circuit_breaker_failure_threshold,
            "circuit_breaker_timeout": self.scraper.circuit_breaker_timeout,
            "request_delay": self.scraper.base_request_delay,
            "max_concurrent_requests": self.scraper.max_concurrent_requests,
            "page_load_timeout": self.scraper.page_load_timeout,
            "element_wait_timeout": self.scraper.element_wait_timeout,
            "headless": self.scraper.headless_browser,
            "viewport": self.scraper.browser_viewport,
            "user_agents": self.scraper.custom_user_agents,
            "use_random_user_agents": self.scraper.use_random_user_agents,
        }
        
        # Specific settings for different scraper types
        if scraper_type == "fortune":
            base_settings.update({
                "request_delay": max(1.0, self.scraper.base_request_delay),  # Slower for Fortune
                "max_concurrent_requests": 1,  # Sequential for reliability
            })
        elif scraper_type == "portfolio":
            base_settings.update({
                "max_concurrent_requests": min(5, self.scraper.max_concurrent_requests),
            })
        elif scraper_type == "team":
            base_settings.update({
                "request_delay": self.scraper.base_request_delay * 1.5,  # Slower for team pages
            })
        
        return base_settings
    
    def get_database_settings(self) -> Dict[str, Any]:
        """Get database configuration settings"""
        return {
            "connection_pool_size": self.database.connection_pool_size,
            "connection_pool_max_overflow": self.database.connection_pool_max_overflow,
            "connection_pool_timeout": self.database.connection_pool_timeout,
            "query_timeout": self.database.query_timeout,
            "batch_size": self.database.batch_size,
            "enable_change_tracking": self.database.enable_change_tracking,
            "change_retention_days": self.database.change_retention_days,
        }
    
    def get_validation_settings(self) -> Dict[str, Any]:
        """Get validation configuration settings"""
        return {
            "min_company_name_length": self.validation.min_company_name_length,
            "max_company_name_length": self.validation.max_company_name_length,
            "required_company_fields": self.validation.required_company_fields,
            "min_member_name_length": self.validation.min_member_name_length,
            "max_member_name_length": self.validation.max_member_name_length,
            "required_member_fields": self.validation.required_member_fields,
            "validate_urls": self.validation.validate_urls,
            "url_timeout": self.validation.url_timeout,
            "min_data_quality_score": self.validation.min_data_quality_score,
        }
    
    def get_monitoring_settings(self) -> Dict[str, Any]:
        """Get monitoring configuration settings"""
        return {
            "health_check_interval": self.monitoring.health_check_interval,
            "health_check_timeout": self.monitoring.health_check_timeout,
            "enable_metrics": self.monitoring.enable_metrics,
            "metrics_port": self.monitoring.metrics_port,
            "log_level": self.monitoring.log_level,
            "log_format": self.monitoring.log_format,
            "log_retention_days": self.monitoring.log_retention_days,
            "enable_structured_logging": self.monitoring.enable_structured_logging,
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert all settings to a dictionary"""
        return {
            "environment": self.environment,
            "scraper": {
                "max_retry_attempts": self.scraper.max_retry_attempts,
                "retry_backoff_factor": self.scraper.retry_backoff_factor,
                "circuit_breaker_failure_threshold": self.scraper.circuit_breaker_failure_threshold,
                "circuit_breaker_timeout": self.scraper.circuit_breaker_timeout,
                "base_request_delay": self.scraper.base_request_delay,
                "max_concurrent_requests": self.scraper.max_concurrent_requests,
                "page_load_timeout": self.scraper.page_load_timeout,
                "element_wait_timeout": self.scraper.element_wait_timeout,
                "headless_browser": self.scraper.headless_browser,
                "browser_viewport": self.scraper.browser_viewport,
            },
            "database": {
                "connection_pool_size": self.database.connection_pool_size,
                "connection_pool_max_overflow": self.database.connection_pool_max_overflow,
                "connection_pool_timeout": self.database.connection_pool_timeout,
                "query_timeout": self.database.query_timeout,
                "batch_size": self.database.batch_size,
            },
            "monitoring": {
                "log_level": self.monitoring.log_level,
                "enable_metrics": self.monitoring.enable_metrics,
                "health_check_interval": self.monitoring.health_check_interval,
            },
            "security": {
                "verify_ssl": self.security.verify_ssl,
                "rate_limit_window": self.security.rate_limit_window,
                "max_requests_per_window": self.security.max_requests_per_window,
            },
            "performance": {
                "enable_response_caching": self.performance.enable_response_caching,
                "cache_ttl": self.performance.cache_ttl,
                "enable_compression": self.performance.enable_compression,
            }
        }

# Global settings instance
settings = AppSettings()

# Convenience functions
def get_scraper_config(scraper_type: str = "default") -> Dict[str, Any]:
    """Get configuration for a specific scraper type"""
    return settings.get_scraper_settings(scraper_type)

def get_database_config() -> Dict[str, Any]:
    """Get database configuration"""
    return settings.get_database_settings()

def get_validation_config() -> Dict[str, Any]:
    """Get validation configuration"""
    return settings.get_validation_settings()

def get_monitoring_config() -> Dict[str, Any]:
    """Get monitoring configuration"""
    return settings.get_monitoring_settings()

def is_production() -> bool:
    """Check if running in production environment"""
    return settings.environment == "production"

def is_development() -> bool:
    """Check if running in development environment"""
    return settings.environment == "development"

def is_testing() -> bool:
    """Check if running in testing environment"""
    return settings.environment == "testing"
