"""
Logging configuration for VC Scraper
"""
import logging
import sys
from typing import Optional
import structlog
import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration

def setup_logging(
    level: str = "INFO",
    format_string: Optional[str] = None,
    enable_json: bool = True,
    sentry_config: Optional[dict] = None
) -> logging.Logger:
    """
    Set up application logging with structured logging support
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        format_string: Custom format string for logs
        enable_json: Whether to use JSON formatting
        sentry_config: Sentry configuration for error tracking
    
    Returns:
        Configured logger instance
    """
    # Set logging level
    log_level = getattr(logging, level.upper(), logging.INFO)
    
    # Configure root logger
    logging.basicConfig(
        level=log_level,
        stream=sys.stdout,
        format=format_string or "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Setup Sentry if configuration provided
    if sentry_config and sentry_config.get("dsn"):
        sentry_logging = LoggingIntegration(
            level=logging.INFO,        # Capture info and above as breadcrumbs
            event_level=logging.ERROR  # Send errors as events
        )
        
        sentry_sdk.init(
            integrations=[sentry_logging],
            **sentry_config
        )
    
    # Configure structured logging if enabled
    if enable_json:
        # Configure structlog
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                structlog.processors.JSONRenderer()
            ],
            wrapper_class=structlog.stdlib.LoggerAdapter,
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )
    
    # Create and return logger
    logger = logging.getLogger("vc_scraper")
    logger.info(f"Logging configured with level: {level}")
    
    return logger

def get_logger(name: str = None) -> logging.Logger:
    """Get a logger instance with optional name"""
    if name:
        return logging.getLogger(f"vc_scraper.{name}")
    return logging.getLogger("vc_scraper")
