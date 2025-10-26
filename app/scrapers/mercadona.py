import re
import logging
from typing import List, Dict, Any, Optional
from lxml import etree
from decimal import Decimal
from datetime import datetime, timezone

from app.scrapers.base import BaseScraper
from app.config import settings

logger = logging.getLogger(__name__)


class MercadonaScraper(BaseScraper):
    """Mercadona supermarket scraper"""
    
    def __init__(self):
        super().__init__("mercadona")
        self.base_url = settings.mercadona_base_url
        self.sitemap_url = settings.mercadona_sitemap_url
        self.api_base_url = settings.mercadona_api_url
    
    async def fetch_sitemap_products(self) -> List[int]:
        """Fetch all product IDs from Mercadona sitemap"""
        logger.info(f"Fetching sitemap from {self.sitemap_url}")
        
        response = await self.make_request(self.sitemap_url)
        if not response:
            logger.error("Failed to fetch sitemap")
            return []
        
        try:
            # Parse XML sitemap
            root = etree.fromstring(response.content)
            
            # Extract product IDs from URLs
            product_ids = []
            for url_elem in root.xpath("//sitemap:loc", namespaces={"sitemap": "http://www.sitemaps.org/schemas/sitemap/0.9"}):
                url = url_elem.text
                if url and "/product/" in url:
                    # Extract product ID from URL like: /product/10005/chocolate-liquido-taza-hacendado-brick
                    match = re.search(r'/product/(\d+)/', url)
                    if match:
                        product_id = int(match.group(1))
                        product_ids.append(product_id)
            
            logger.info(f"Found {len(product_ids)} products in sitemap")
            return product_ids
            
        except etree.XMLSyntaxError as e:
            logger.error(f"Failed to parse sitemap XML: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error parsing sitemap: {e}")
            return []
    
    async def fetch_product_details(self, product_id: int) -> Optional[Dict[str, Any]]:
        """Fetch detailed product information from Mercadona API"""
        url = f"{self.api_base_url}/{product_id}/"
        
        response = await self.make_request(url)
        if not response:
            logger.debug(f"Failed to fetch product {product_id}")
            return None
        
        try:
            data = response.json()
            
            # Extract product information from API response
            product_data = self._parse_product_data(data, product_id)
            return product_data
            
        except Exception as e:
            logger.error(f"Error parsing product {product_id}: {e}")
            return None
    
    def _parse_product_data(self, api_data: Dict[str, Any], product_id: int) -> Optional[Dict[str, Any]]:
        """Parse product data from Mercadona API response"""
        try:
            # Check if product is published/available
            if not api_data.get("published", False):
                logger.debug(f"Product {product_id} is not published")
                return None
            
            # Extract basic product information
            product_data = {
                "product_id": product_id,
                "supermarket_name": self.supermarket_name,
                "product_type": None,  # Not available in Mercadona API
                "product_name": api_data.get("display_name", ""),
                "product_description": api_data.get("details", {}).get("description"),
                "product_brand": api_data.get("brand"),
                "product_pack_size_description": api_data.get("packaging"),
                
                # Pricing information
                "product_price_amount": self._parse_price(api_data.get("price_instructions", {}).get("unit_price")),
                "product_currency": "EUR",  # Mercadona uses EUR
                "product_unit_price_amount": self._parse_price(api_data.get("price_instructions", {}).get("reference_price")),
                "product_unit_price_currency": "EUR",
                "product_unit_price_unit": api_data.get("price_instructions", {}).get("reference_format"),
                
                # Product attributes
                "product_available": api_data.get("published", True),
                "product_alcohol": api_data.get("badges", {}).get("requires_age_check", False),
                "product_cooking_guidelines": api_data.get("details", {}).get("usage_instructions"),
                "product_categories": self._parse_categories(api_data.get("categories", [])),
                
                # Timestamps
                "last_updated": datetime.now(timezone.utc),
                "created_at": datetime.now(timezone.utc),
            }
            
            return product_data
            
        except Exception as e:
            logger.error(f"Error parsing product data for {product_id}: {e}")
            return None
    
    def _parse_price(self, price_data: Any) -> Optional[Decimal]:
        """Parse price from various possible formats"""
        if price_data is None:
            return None
        
        try:
            # Handle different price formats
            if isinstance(price_data, (int, float)):
                return Decimal(str(price_data))
            elif isinstance(price_data, str):
                # Remove currency symbols and clean the string
                clean_price = re.sub(r'[^\d.,]', '', price_data)
                clean_price = clean_price.replace(',', '.')
                return Decimal(clean_price)
            elif isinstance(price_data, dict):
                # If price is an object with amount field
                amount = price_data.get("amount") or price_data.get("value")
                if amount is not None:
                    return Decimal(str(amount))
            
            return None
            
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not parse price {price_data}: {e}")
            return None
    
    def _parse_categories(self, categories_data: List[Any]) -> List[str]:
        """Parse categories from Mercadona API response"""
        if not categories_data:
            return []
        
        try:
            categories = []
            
            def extract_category_names(category_list):
                """Recursively extract category names from nested structure"""
                for category in category_list:
                    if isinstance(category, dict):
                        name = category.get("name")
                        if name:
                            categories.append(name)
                        # Recursively process nested categories
                        nested_categories = category.get("categories", [])
                        if nested_categories:
                            extract_category_names(nested_categories)
            
            extract_category_names(categories_data)
            return categories
            
        except Exception as e:
            logger.warning(f"Error parsing categories: {e}")
            return []
    
    async def test_connection(self) -> bool:
        """Test if we can connect to Mercadona API"""
        try:
            # Try to fetch a single product to test connection
            test_product_id = 10005  # Use a known product ID from sitemap
            response = await self.make_request(f"{self.api_base_url}/{test_product_id}/")
            return response is not None and response.status_code in [200, 404]  # 404 is OK for non-existent product
            
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
