"""
Retry logic and error handling utilities
"""
import asyncio
import functools
import random
import time
from typing import Any, Callable, Type, Union, Optional
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class RetryError(Exception):
    """Raised when all retry attempts have been exhausted"""
    pass

def with_retry(
    max_attempts: int = 3,
    backoff_factor: float = 2.0,
    jitter: bool = True,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    exceptions: Union[Type[Exception], tuple] = Exception,
    reraise_exceptions: Union[Type[Exception], tuple] = None
):
    """
    Decorator for adding retry logic to functions
    
    Args:
        max_attempts: Maximum number of attempts
        backoff_factor: Multiplier for delay between attempts
        jitter: Whether to add random jitter to delays
        initial_delay: Initial delay in seconds
        max_delay: Maximum delay between attempts
        exceptions: Exception types to catch and retry
        reraise_exceptions: Exception types to reraise immediately
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs) -> Any:
            last_exception = None
            delay = initial_delay
            
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    # Reraise certain exceptions immediately
                    if reraise_exceptions and isinstance(e, reraise_exceptions):
                        raise
                    
                    # Check if this exception should be retried
                    if not isinstance(e, exceptions):
                        raise
                    
                    # Don't delay on the last attempt
                    if attempt == max_attempts - 1:
                        logger.warning(
                            f"Final retry attempt {attempt + 1} failed for {func.__name__}: {e}"
                        )
                        break
                    
                    # Calculate delay with jitter
                    actual_delay = delay
                    if jitter:
                        actual_delay *= (0.5 + random.random())
                    actual_delay = min(actual_delay, max_delay)
                    
                    logger.warning(
                        f"Attempt {attempt + 1} failed for {func.__name__}: {e}. "
                        f"Retrying in {actual_delay:.2f} seconds..."
                    )
                    
                    await asyncio.sleep(actual_delay)
                    delay *= backoff_factor
            
            # All attempts failed
            raise RetryError(
                f"Function {func.__name__} failed after {max_attempts} attempts. "
                f"Last error: {last_exception}"
            ) from last_exception
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs) -> Any:
            last_exception = None
            delay = initial_delay
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    # Reraise certain exceptions immediately
                    if reraise_exceptions and isinstance(e, reraise_exceptions):
                        raise
                    
                    # Check if this exception should be retried
                    if not isinstance(e, exceptions):
                        raise
                    
                    # Don't delay on the last attempt
                    if attempt == max_attempts - 1:
                        logger.warning(
                            f"Final retry attempt {attempt + 1} failed for {func.__name__}: {e}"
                        )
                        break
                    
                    # Calculate delay with jitter
                    actual_delay = delay
                    if jitter:
                        actual_delay *= (0.5 + random.random())
                    actual_delay = min(actual_delay, max_delay)
                    
                    logger.warning(
                        f"Attempt {attempt + 1} failed for {func.__name__}: {e}. "
                        f"Retrying in {actual_delay:.2f} seconds..."
                    )
                    
                    time.sleep(actual_delay)
                    delay *= backoff_factor
            
            # All attempts failed
            raise RetryError(
                f"Function {func.__name__} failed after {max_attempts} attempts. "
                f"Last error: {last_exception}"
            ) from last_exception
        
        # Return appropriate wrapper based on whether function is async
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator

class CircuitBreaker:
    """
    Circuit breaker pattern implementation for handling remote service failures
    """
    
    def __init__(
        self,
        failure_threshold: int = 5,
        timeout: float = 60.0,
        expected_exception: Type[Exception] = Exception
    ):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.expected_exception = expected_exception
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
    
    def __call__(self, func: Callable) -> Callable:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs) -> Any:
            if self.state == "OPEN":
                if self._should_try_reset():
                    self.state = "HALF_OPEN"
                    self.failure_count = 0
                else:
                    raise Exception(f"Circuit breaker is OPEN. Last failure: {self.last_failure_time}")
            
            try:
                result = await func(*args, **kwargs)
                self._on_success()
                return result
            except self.expected_exception as e:
                self._on_failure()
                raise
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs) -> Any:
            if self.state == "OPEN":
                if self._should_try_reset():
                    self.state = "HALF_OPEN"
                    self.failure_count = 0
                else:
                    raise Exception(f"Circuit breaker is OPEN. Last failure: {self.last_failure_time}")
            
            try:
                result = func(*args, **kwargs)
                self._on_success()
                return result
            except self.expected_exception as e:
                self._on_failure()
                raise
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    def _should_try_reset(self) -> bool:
        """Check if enough time has passed to try resetting the circuit"""
        if self.last_failure_time is None:
            return True
        return time.time() - self.last_failure_time >= self.timeout
    
    def _on_success(self):
        """Handle successful execution"""
        self.failure_count = 0
        self.state = "CLOSED"
        self.last_failure_time = None
    
    def _on_failure(self):
        """Handle failed execution"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
            logger.warning(
                f"Circuit breaker opened after {self.failure_count} failures. "
                f"Timeout: {self.timeout} seconds"
            )

# Common exceptions that might need special handling
class ScrapingError(Exception):
    """Base exception for scraping-related errors"""
    pass

class RateLimitError(ScrapingError):
    """Raised when rate limiting is detected"""
    pass

class ParseError(ScrapingError):
    """Raised when parsing fails"""
    pass

class ValidationError(ScrapingError):
    """Raised when data validation fails"""
    pass
