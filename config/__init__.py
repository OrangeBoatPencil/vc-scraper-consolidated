"""
Configuration package for the VC Scraper application.

This package provides centralized configuration management with support for
environment variables and YAML configuration files.
"""

from .settings import (
    ApplicationSettings,
    settings,
    get_settings,
    get_site_config,
    get_active_sites,
    get_supabase_credentials,
    Environment,
    LogLevel
)

__all__ = [
    'ApplicationSettings',
    'settings',
    'get_settings',
    'get_site_config',
    'get_active_sites',
    'get_supabase_credentials',
    'Environment',
    'LogLevel'
]
