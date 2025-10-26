import asyncio
import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import httpx
import random
from asyncio_throttle import Throttler

from app.config import settings

logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """Base class for all supermarket scrapers"""
    
    def __init__(self, supermarket_name: str):
        self.supermarket_name = supermarket_name
        self.base_url = ""
        self.timeout = settings.request_timeout
        self.concurrent_requests = settings.concurrent_requests
        
        # Create HTTP client with proper headers
        self.client = httpx.AsyncClient(
            timeout=self.timeout,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9,ca;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
            }
        )
        
        # Create throttler for rate limiting
        self.throttler = Throttler(rate_limit=self.concurrent_requests, period=1.0)
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()
    
    @abstractmethod
    async def fetch_sitemap_products(self) -> List[int]:
        """Fetch all product IDs from sitemap"""
        pass
    
    @abstractmethod
    async def fetch_product_details(self, product_id: int) -> Optional[Dict[str, Any]]:
        """Fetch detailed product information"""
        pass
    
    async def make_request(self, url: str, retries: int = 3) -> Optional[httpx.Response]:
        """Make HTTP request with retry logic and rate limiting"""
        async with self.throttler:
            for attempt in range(retries):
                try:
                    # Add random jitter to avoid patterns
                    jitter = random.uniform(0.01, 0.1)
                    await asyncio.sleep(jitter)
                    
                    response = await self.client.get(url)
                    response.raise_for_status()
                    return response
                    
                except httpx.HTTPStatusError as e:
                    logger.debug(f"HTTP error {e.response.status_code} for {url} (attempt {attempt + 1})")
                    if e.response.status_code == 429:  # Rate limited
                        wait_time = 2 ** attempt + random.uniform(1, 3)
                        logger.info(f"Rate limited, waiting {wait_time:.1f} seconds")
                        await asyncio.sleep(wait_time)
                    elif e.response.status_code >= 500:  # Server error
                        wait_time = 2 ** attempt
                        await asyncio.sleep(wait_time)
                    else:
                        break  # Client error, don't retry
                        
                except httpx.RequestError as e:
                    logger.warning(f"Request error for {url} (attempt {attempt + 1}): {e}")
                    if attempt < retries - 1:
                        wait_time = 2 ** attempt
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(f"Failed to fetch {url} after {retries} attempts: {e}")
                        return None
                        
                except Exception as e:
                    logger.error(f"Unexpected error for {url}: {e}")
                    return None
            
            return None
    
    async def process_products_batch(
        self, 
        product_ids: List[int], 
        batch_duration_minutes: int = None
    ) -> Dict[str, int]:
        """Process a batch of products with rate limiting - returns product data for service layer to handle"""
        if batch_duration_minutes is None:
            batch_duration_minutes = settings.batch_duration_minutes
        
        batch_size = len(product_ids)
        if batch_size == 0:
            return {"processed": 0, "updated": 0, "errors": 0}
        
        # Calculate delay between requests to spread over batch_duration_minutes
        total_seconds = batch_duration_minutes * 60
        delay_between_requests = total_seconds / batch_size
        
        logger.info(f"Processing {batch_size} products over {batch_duration_minutes} minutes")
        logger.info(f"Delay between requests: {delay_between_requests:.2f} seconds")
        
        results = {"processed": 0, "updated": 0, "errors": 0}
        
        # Process products concurrently in smaller batches
        semaphore = asyncio.Semaphore(self.concurrent_requests)
        
        async def process_single_product(product_id: int):
            async with semaphore:
                try:
                    product_data = await self.fetch_product_details(product_id)
                    if product_data:
                        results["processed"] += 1
                        # Service layer will handle database updates
                        results["updated"] += 1
                    else:
                        results["errors"] += 1
                        
                except Exception as e:
                    logger.error(f"Error processing product {product_id}: {e}")
                    results["errors"] += 1
        
        # Create tasks for all products
        tasks = [process_single_product(pid) for pid in product_ids]
        
        # Process with delay between batches
        batch_size_concurrent = self.concurrent_requests
        for i in range(0, len(tasks), batch_size_concurrent):
            batch_tasks = tasks[i:i + batch_size_concurrent]
            await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            # Wait before next batch (except for the last batch)
            if i + batch_size_concurrent < len(tasks):
                await asyncio.sleep(delay_between_requests * batch_size_concurrent)
        
        logger.info(f"Batch processing completed: {results}")
        return results
    
    def calculate_batch_size(self, total_products: int) -> int:
        """Calculate how many products to process in this batch"""
        return max(1, int(total_products * settings.batch_size_fraction))
    
    def get_next_batch_products(
        self, 
        all_product_ids: List[int], 
        last_processed_index: int = 0
    ) -> tuple[List[int], int]:
        """Get the next batch of products to process"""
        batch_size = self.calculate_batch_size(len(all_product_ids))
        
        # Get products starting from last_processed_index
        start_index = last_processed_index
        end_index = min(start_index + batch_size, len(all_product_ids))
        
        batch_products = all_product_ids[start_index:end_index]
        next_index = end_index if end_index < len(all_product_ids) else 0  # Wrap around
        
        return batch_products, next_index
