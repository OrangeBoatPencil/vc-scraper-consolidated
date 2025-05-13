# Circuit Breaker Integration for VC Scraper

This document explains the circuit breaker integration that has been added to improve the resilience of the VC scraper system.

## What is a Circuit Breaker?

A circuit breaker is a design pattern that prevents an application from repeatedly attempting operations that are likely to fail. It acts as a protective mechanism that:

1. **Prevents cascading failures** when a service is down
2. **Reduces resource waste** on repeated failed attempts
3. **Provides faster failure detection** instead of waiting for timeouts
4. **Allows automatic recovery** when services come back online

## Circuit Breaker States

The circuit breaker has three states:

1. **CLOSED**: Normal operation, all requests are allowed
2. **OPEN**: Service appears to be failing, requests are blocked immediately
3. **HALF_OPEN**: Testing if the service has recovered, limited requests allowed

## Implementation Details

### Configuration

Circuit breakers are automatically initialized in the `BaseScraper` class with the following settings:

```python
# HTTP circuit breaker (for aiohttp requests)
_http_circuit_breaker = CircuitBreaker(
    failure_threshold=5,      # Open after 5 consecutive failures
    timeout=300.0,           # Stay open for 5 minutes
    expected_exception=(aiohttp.ClientError, asyncio.TimeoutError, RateLimitError)
)

# Playwright circuit breaker (for browser automation)
_playwright_circuit_breaker = CircuitBreaker(
    failure_threshold=3,      # Open after 3 consecutive failures
    timeout=600.0,           # Stay open for 10 minutes
    expected_exception=(Exception,)  # More general for browser issues
)
```

### How It Works

1. **Fetch Methods**: Both `fetch_with_requests()` and `fetch_with_playwright()` are now wrapped with circuit breakers
2. **Failure Counting**: When requests fail, the circuit breaker counts the failures
3. **Threshold Reached**: After the failure threshold is reached, the circuit breaker opens
4. **Immediate Failures**: While open, all requests fail immediately without attempting the network call
5. **Recovery Testing**: After the timeout period, the circuit breaker enters HALF_OPEN state to test recovery

### Benefits

1. **Faster Failure Detection**: When a site is down, you know immediately instead of waiting for timeouts
2. **Resource Conservation**: No wasted requests to failed services
3. **Automatic Recovery**: The system automatically checks when services are back online
4. **Better Error Messages**: Clear indication when circuit breakers are open

## Usage Example

```python
from lib.scrapers.portfolio_scraper import PortfolioScraper

async with PortfolioScraper() as scraper:
    try:
        companies = await scraper.scrape("https://vc-firm.com/portfolio")
    except Exception as e:
        # Check circuit breaker status
        stats = scraper.get_scraping_stats()
        
        if stats['http_circuit_breaker_state'] == 'OPEN':
            print("HTTP circuit breaker is open - service appears to be down")
            print(f"Will retry automatically in {scraper._http_circuit_breaker.timeout} seconds")
```

## Monitoring Circuit Breaker Status

The `get_scraping_stats()` method now includes circuit breaker information:

```python
stats = scraper.get_scraping_stats()
print(f"HTTP circuit breaker: {stats['http_circuit_breaker_state']}")
print(f"Playwright circuit breaker: {stats['playwright_circuit_breaker_state']}")
print(f"HTTP failures: {stats['http_failures']}")
print(f"Playwright failures: {stats['playwright_failures']}")
```

## Testing Circuit Breakers

Run the circuit breaker demo to see how it behaves:

```bash
python examples/circuit_breaker_demo.py
```

This script will:
1. Attempt to scrape invalid URLs to trigger failures
2. Show how the circuit breaker opens after reaching the failure threshold
3. Demonstrate immediate failure responses when the circuit is open

## Customization

You can customize circuit breaker settings by modifying the `BaseScraper` class initialization:

```python
class CustomScraper(BaseScraper):
    def __init__(self, config=None):
        super().__init__(config)
        
        # Override circuit breaker settings
        self._http_circuit_breaker = CircuitBreaker(
            failure_threshold=10,    # More lenient
            timeout=60.0,           # Shorter timeout
            expected_exception=(aiohttp.ClientError,)
        )
```

## Best Practices

1. **Monitor Circuit Breaker States**: Include circuit breaker status in your monitoring
2. **Set Appropriate Thresholds**: Balance between quick failure detection and false positives
3. **Log Circuit Breaker Events**: Track when circuit breakers open and close
4. **Handle Open Circuits Gracefully**: Provide meaningful feedback when services are down
5. **Test Regularly**: Use the demo script to verify circuit breaker behavior

## Integration with Retry Logic

Circuit breakers work alongside the existing retry logic:

1. **First**: Retry logic attempts to recover from transient failures
2. **Then**: If retries are exhausted, circuit breaker counts it as a failure
3. **Finally**: After multiple failures, circuit breaker prevents further attempts

This two-layer approach provides both resilience for transient issues and protection from persistent failures.
