# Settings Configuration

This module provides centralized configuration management for the VC Scraper application using Pydantic models for type safety and validation.

## Features

- **Environment Variables**: Automatic loading from `.env` files
- **YAML Configuration**: Site-specific configurations from `sites.yaml`
- **Type Safety**: Pydantic models with validation
- **Multiple Environments**: Support for development, staging, and production
- **Site-Specific Overrides**: Custom scraping settings per domain
- **Validation**: Built-in validation for all configuration values

## Usage

### Basic Usage

```python
from config import settings

# Access Supabase configuration
print(settings.supabase.url)
print(settings.supabase.pool_size)

# Get scraping settings
print(settings.scraping.max_concurrent_requests)
print(settings.scraping.request_delay)

# Check environment
if settings.is_production():
    print("Running in production mode")
```

### Working with Sites

```python
from config import get_active_sites, get_site_config

# Get all active sites
active_sites = get_active_sites()
for site in active_sites:
    print(f"Scraping {site.name} at {site.portfolio_url}")

# Get specific site configuration
site = get_site_config("Example VC")
if site:
    print(f"Team URL: {site.team_url}")
```

### Site-Specific Configurations

```python
# Get scraping config with site-specific overrides
domain = "fortune.com"
config = settings.get_scraping_config_for_site(domain)
print(f"Request delay for {domain}: {config['request_delay']}")
```

## Configuration Files

### Environment Variables (`.env`)

```bash
# Required
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_ANON_KEY=your-anon-key
SUPABASE_SERVICE_ROLE_KEY=your-service-key

# Optional
OPENAI_API_KEY=your-openai-key
ENVIRONMENT=production
LOG_LEVEL=INFO
```

### Sites Configuration (`config/sites.yaml`)

```yaml
vc_sites:
  - name: "Example VC"
    url: "https://example-vc.com"
    portfolio_url: "https://example-vc.com/portfolio"
    team_url: "https://example-vc.com/team"
    active: true

scraping:
  max_concurrent_requests: 5
  request_delay: 1.0
  site_configs:
    "fortune.com":
      request_delay: 2.0
      use_playwright: true
```

## Configuration Sections

### 1. Supabase Settings
- Database connection configuration
- Connection pooling settings
- Authentication keys

### 2. Scraping Settings
- Request delays and timeouts
- Concurrent request limits
- Browser automation settings
- User agent configuration

### 3. Site Configurations
- VC firm URLs and endpoints
- Site-specific overrides
- Activity status

### 4. Database Settings
- Table configurations
- Change tracking settings
- Cleanup policies

### 5. Logging Settings
- Log levels and formats
- File logging configuration
- Structured logging support

### 6. Monitoring Settings
- Health check endpoints
- Metrics collection
- Success criteria

### 7. MCP Settings
- Server configurations
- Port assignments
- Feature toggles

## Validation

The settings module includes built-in validation:

```python
# Validate all required settings
try:
    settings.validate_required_settings()
    print("✅ Configuration valid")
except ValueError as e:
    print(f"❌ Configuration error: {e}")
```

## Testing

Test your configuration:

```bash
# Test settings configuration
python scripts/test_settings.py

# Run example usage
python examples/settings_usage.py
```

## Environment-Specific Behavior

The settings module automatically adjusts behavior based on the environment:

- **Development**: Enables debug logging, uses default values
- **Staging**: Production-like settings with enhanced logging
- **Production**: Optimized settings, error tracking enabled

## Adding New Settings

To add new configuration options:

1. Add the field to the appropriate Pydantic model
2. Add validation if needed
3. Update the YAML configuration schema
4. Add environment variable support if required

Example:

```python
class ScrapingSettings(BaseModel):
    # Existing fields...
    
    # New field
    enable_caching: bool = Field(False, description="Enable response caching")
    cache_duration: int = Field(3600, description="Cache duration in seconds")
    
    @validator('cache_duration')
    def validate_cache_duration(cls, v):
        if v < 60:
            raise ValueError('cache_duration must be at least 60 seconds')
        return v
```

## Best Practices

1. **Use Type Hints**: Always include type hints for better IDE support
2. **Add Validation**: Include validators for critical settings
3. **Document Defaults**: Use Field descriptions to document defaults
4. **Environment Variables**: Use env variables for sensitive data
5. **YAML for Structure**: Use YAML for complex configuration structures
6. **Validation**: Always validate settings on startup

## Troubleshooting

### Common Issues

1. **Missing Environment Variables**
   ```bash
   # Check if .env file exists and contains required variables
   cat .env
   ```

2. **YAML Parsing Errors**
   ```bash
   # Validate YAML syntax
   python -c "import yaml; yaml.safe_load(open('config/sites.yaml'))"
   ```

3. **Validation Errors**
   ```python
   # Check specific validation errors
   from config import settings
   settings.validate_required_settings()
   ```

## Integration with Other Components

The settings module is designed to integrate seamlessly with:

- **FastMCP servers**: Server ports and configurations
- **Database connections**: Supabase credentials and settings
- **Scrapers**: Site-specific configurations and limits
- **Monitoring**: Health checks and metrics endpoints
- **Deployment**: Fly.io and environment-specific settings
