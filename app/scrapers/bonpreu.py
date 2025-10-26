import re
import logging
from typing import List, Dict, Any, Optional
from lxml import etree
from decimal import Decimal
from datetime import datetime, timezone

from app.scrapers.base import BaseScraper
from app.config import settings

logger = logging.getLogger(__name__)


class BonpreuScraper(BaseScraper):
    """Bonpreu supermarket scraper"""
    
    def __init__(self):
        super().__init__("bonpreu")
        self.base_url = settings.bonpreu_base_url
        self.sitemap_url = settings.bonpreu_sitemap_url
        self.api_base_url = f"{self.base_url}/api/webproductpagews/v5/products/bop"
    
    async def fetch_sitemap_products(self) -> List[int]:
        """Fetch all product IDs from Bonpreu sitemap"""
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
                if url and "/products/" in url:
                    # Extract product ID from URL like: /products/product-name/12345
                    match = re.search(r'/products/[^/]+/(\d+)/?$', url)
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
        """Fetch detailed product information from Bonpreu API"""
        url = f"{self.api_base_url}?retailerProductId={product_id}"
        
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
        """Parse product data from Bonpreu API response"""
        try:
            # Navigate through the API response structure
            # The exact structure may need to be adjusted based on actual API response
            product_info = api_data.get("product", {})
            if not product_info:
                logger.warning(f"No product data found for product {product_id}")
                return None
            
            # Extract basic product information
            product_data = {
                "product_id": product_id,
                "supermarket_name": self.supermarket_name,
                "product_type": product_info.get("type"),
                "product_name": product_info.get("name", ""),
                "product_description": api_data.get("bopData", {}).get("detailedDescription"),
                "product_brand": product_info.get("brand"),
                "product_pack_size_description": product_info.get("packSizeDescription"),
                
                # Pricing information
                "product_price_amount": self._parse_price(product_info.get("price")),
                "product_currency": "EUR",  # Bonpreu uses EUR
                "product_unit_price_amount": self._parse_unit_price_amount(product_info.get("unitPrice")),
                "product_unit_price_currency": "EUR",
                "product_unit_price_unit": self._parse_unit_price_unit(product_info.get("unitPrice")),
                
                # Product attributes
                "product_available": product_info.get("available", True),
                "product_alcohol": product_info.get("alcohol", False),
                "product_cooking_guidelines": product_info.get("cookingGuidelines"),
                "product_categories": self._parse_categories(product_info.get("categoryPath", [])),
                
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
    
    def _parse_unit_price_amount(self, unit_price_data: Any) -> Optional[Decimal]:
        """Parse unit price amount from nested unitPrice object"""
        if not unit_price_data or not isinstance(unit_price_data, dict):
            return None
        
        try:
            price_data = unit_price_data.get("price", {})
            if isinstance(price_data, dict):
                amount = price_data.get("amount")
                if amount is not None:
                    return self._parse_price(amount)
            
            return None
            
        except Exception as e:
            logger.warning(f"Error parsing unit price amount: {e}")
            return None
    
    def _parse_unit_price_unit(self, unit_price_data: Any) -> Optional[str]:
        """Parse unit price unit from nested unitPrice object"""
        if not unit_price_data or not isinstance(unit_price_data, dict):
            return None
        
        try:
            unit = unit_price_data.get("unit")
            if unit:
                # Clean up the unit string (remove 'fop.price.per.' prefix if present)
                if unit.startswith("fop.price.per."):
                    unit = unit.replace("fop.price.per.", "")
                return unit
            
            return None
            
        except Exception as e:
            logger.warning(f"Error parsing unit price unit: {e}")
            return None
    
    def _parse_categories(self, categories_data: List[Any]) -> List[str]:
        """Parse categories from API response"""
        if not categories_data:
            return []
        
        try:
            categories = []
            for category in categories_data:
                if isinstance(category, str):
                    categories.append(category)
                elif isinstance(category, dict):
                    # Extract category name from object
                    name = category.get("name") or category.get("title")
                    if name:
                        categories.append(name)
            
            return categories
            
        except Exception as e:
            logger.warning(f"Error parsing categories: {e}")
            return []
    
    async def test_connection(self) -> bool:
        """Test if we can connect to Bonpreu API"""
        try:
            # Try to fetch a single product to test connection
            test_product_id = 1  # Use a simple test ID
            response = await self.make_request(f"{self.api_base_url}?retailerProductId={test_product_id}")
            return response is not None and response.status_code in [200, 404]  # 404 is OK for non-existent product
            
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
