import re
import logging
import json
import gzip
from typing import List, Dict, Any, Optional
from lxml import etree, html
from decimal import Decimal
from datetime import datetime, timezone
import httpx

from app.scrapers.base import BaseScraper
from app.config import settings

logger = logging.getLogger(__name__)


class LidlScraper(BaseScraper):
    """Lidl supermarket scraper"""
    
    def __init__(self):
        super().__init__("lidl")
        self.base_url = settings.lidl_base_url
        self.sitemap_url = settings.lidl_sitemap_url
        # Replace base client (which has JSON headers) with HTML-optimized client
        # Base class __aexit__ will close this client properly
        self.client = httpx.AsyncClient(
            follow_redirects=True,  # Explicitly enable redirects (default in httpx)
            timeout=self.timeout,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "es-ES,es;q=0.9,ca;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
            }
        )
    
    async def fetch_sitemap_products(self) -> List[int]:
        """Fetch all product IDs from Lidl gzipped sitemap"""
        logger.info(f"Fetching sitemap from {self.sitemap_url}")
        
        response = await self.make_request(self.sitemap_url)
        if not response:
            logger.error("Failed to fetch sitemap")
            return []
        
        try:
            # Handle gzip decompression manually for .gz files
            # httpx may not auto-decompress if server doesn't set Content-Encoding header
            # Since URL ends in .gz, we expect gzipped content
            content = response.content
            try:
                content = gzip.decompress(content)
                logger.debug("Successfully decompressed gzip sitemap")
            except (gzip.BadGzipFile, Exception) as e:
                # If decompression fails, check if it's already plain text
                # (some servers may auto-decompress)
                if content[:1] == b'<':
                    # Already XML, use as-is
                    logger.debug("Content appears to be plain XML, using as-is")
                else:
                    logger.warning(f"Failed to decompress gzip content: {e}")
                    raise ValueError(f"Unable to decompress sitemap content: {e}")
            
            # Parse XML sitemap
            root = etree.fromstring(content)
            
            # Extract product IDs from URLs
            product_ids = []
            for url_elem in root.xpath("//sitemap:loc", namespaces={"sitemap": "http://www.sitemaps.org/schemas/sitemap/0.9"}):
                url = url_elem.text
                if url:
                    # Handle both formats: /es/product-slug/p4989 and /es/product-slugp4989
                    # Pattern 1: /p{id} at end of URL
                    match = re.search(r'/p(\d+)/?$', url)
                    if not match:
                        # Pattern 2: p{id} at end without preceding slash
                        match = re.search(r'p(\d+)/?$', url)
                    
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
        """Fetch detailed product information from Lidl product page (SSR)"""
        # Use short URL that redirects to full URL
        url = f"{self.base_url}/es/p{product_id}"
        
        response = await self.make_request(url)
        if not response:
            logger.debug(f"Failed to fetch product {product_id}")
            return None
        
        try:
            # Parse HTML content
            doc = html.fromstring(response.text)
            
            # Extract product information from HTML
            product_data = self._parse_product_data(doc, product_id, response.url)
            return product_data
            
        except Exception as e:
            logger.error(f"Error parsing product {product_id}: {e}")
            return None
    
    def _parse_product_data(self, doc: html.HtmlElement, product_id: int, final_url: str) -> Optional[Dict[str, Any]]:
        """Parse product data from HTML document"""
        try:
            # Try to find JSON-LD structured data first (most reliable)
            json_ld_data = self._extract_json_ld(doc)
            if json_ld_data:
                return self._parse_from_json_ld(json_ld_data, product_id)
            
            # Fallback to HTML parsing
            product_name = self._extract_product_name(doc)
            if not product_name:
                logger.warning(f"No product name found for product {product_id}")
                return None
            
            # Extract price
            price_amount = self._extract_price(doc)
            
            # Extract other information
            description = self._extract_description(doc)
            brand = self._extract_brand(doc)
            categories = self._extract_categories(doc)
            unit_price = self._extract_unit_price(doc)
            
            product_data = {
                "product_id": product_id,
                "supermarket_name": self.supermarket_name,
                "product_type": None,
                "product_name": product_name,
                "product_description": description,
                "product_brand": brand,
                "product_pack_size_description": None,
                
                # Pricing information
                "product_price_amount": price_amount,
                "product_currency": "EUR",
                "product_unit_price_amount": unit_price.get("amount") if unit_price else None,
                "product_unit_price_currency": "EUR",
                "product_unit_price_unit": unit_price.get("unit") if unit_price else None,
                
                # Product attributes
                "product_available": True,  # If page loads, assume available
                "product_alcohol": False,
                "product_cooking_guidelines": None,
                "product_categories": categories,
                
                # Timestamps
                "last_updated": datetime.now(timezone.utc),
                "created_at": datetime.now(timezone.utc),
            }
            
            return product_data
            
        except Exception as e:
            logger.error(f"Error parsing product data for {product_id}: {e}")
            return None
    
    def _extract_json_ld(self, doc: html.HtmlElement) -> Optional[Dict[str, Any]]:
        """Extract JSON-LD structured data from page"""
        try:
            # Look for script tags with type application/ld+json
            scripts = doc.xpath('//script[@type="application/ld+json"]')
            for script in scripts:
                try:
                    json_data = json.loads(script.text)
                    # Check if it's product data
                    if isinstance(json_data, dict):
                        if json_data.get("@type") == "Product" or "product" in str(json_data.get("@type", "")).lower():
                            return json_data
                        # Sometimes it's in a list
                        if isinstance(json_data.get("@graph"), list):
                            for item in json_data.get("@graph", []):
                                if item.get("@type") == "Product":
                                    return item
                except (json.JSONDecodeError, TypeError):
                    continue
        except Exception as e:
            logger.debug(f"Error extracting JSON-LD: {e}")
        return None
    
    def _parse_from_json_ld(self, json_ld: Dict[str, Any], product_id: int) -> Optional[Dict[str, Any]]:
        """Parse product data from JSON-LD structured data"""
        try:
            # Extract name
            product_name = json_ld.get("name", "")
            if not product_name:
                return None
            
            # Extract price
            offers = json_ld.get("offers", {})
            if isinstance(offers, list) and len(offers) > 0:
                offers = offers[0]
            
            price = None
            if isinstance(offers, dict):
                price_str = offers.get("price") or offers.get("priceSpecification", {}).get("price")
                if price_str:
                    price = self._parse_price(str(price_str))
            
            # Extract description
            description = json_ld.get("description", "")
            
            # Extract brand
            brand = None
            brand_data = json_ld.get("brand", {})
            if isinstance(brand_data, dict):
                brand = brand_data.get("name")
            elif isinstance(brand_data, str):
                brand = brand_data
            
            # Extract categories (may be in category field)
            categories = []
            category = json_ld.get("category")
            if category:
                if isinstance(category, str):
                    categories = [category]
                elif isinstance(category, list):
                    categories = [str(c) for c in category]
            
            # Extract unit price if available
            unit_price = None
            unit_price_str = offers.get("unitPriceSpecification", {}).get("value") if isinstance(offers, dict) else None
            if unit_price_str:
                unit_price = {
                    "amount": self._parse_price(str(unit_price_str)),
                    "unit": offers.get("unitPriceSpecification", {}).get("unitCode", "")
                }
            
            return {
                "product_id": product_id,
                "supermarket_name": self.supermarket_name,
                "product_type": None,
                "product_name": product_name,
                "product_description": description,
                "product_brand": brand,
                "product_pack_size_description": None,
                "product_price_amount": price,
                "product_currency": "EUR",
                "product_unit_price_amount": unit_price.get("amount") if unit_price else None,
                "product_unit_price_currency": "EUR",
                "product_unit_price_unit": unit_price.get("unit") if unit_price else None,
                "product_available": True,
                "product_alcohol": False,
                "product_cooking_guidelines": None,
                "product_categories": categories,
                "last_updated": datetime.now(timezone.utc),
                "created_at": datetime.now(timezone.utc),
            }
        except Exception as e:
            logger.error(f"Error parsing JSON-LD data: {e}")
            return None
    
    def _extract_product_name(self, doc: html.HtmlElement) -> Optional[str]:
        """Extract product name from HTML"""
        # Try multiple selectors
        selectors = [
            '//h1[@class="product-title"]',
            '//h1[contains(@class, "product")]',
            '//h1',
            '//*[@data-product-name]',
            '//meta[@property="og:title"]/@content',
            '//title'
        ]
        
        for selector in selectors:
            try:
                elements = doc.xpath(selector)
                if elements:
                    if selector.endswith('/@content'):
                        return elements[0].strip()
                    text = elements[0].text_content().strip()
                    if text:
                        return text
            except Exception:
                continue
        
        return None
    
    def _extract_price(self, doc: html.HtmlElement) -> Optional[Decimal]:
        """Extract price from HTML"""
        # Method 1: Try data-price attribute (but exclude threshold-related elements)
        price_elements = doc.xpath('//*[@data-price]')
        for elem in price_elements:
            # Skip if it has data-threshold (financing threshold, not price)
            if 'data-threshold' in elem.attrib:
                continue
            price_str = elem.get('data-price', '')
            if price_str:
                price = self._parse_price(price_str)
                if price and 0 < price < 10000:  # Reasonable price range
                    logger.debug(f"Found price from data-price attribute: {price}")
                    return price
        
        # Method 2: Look for price in JavaScript variables (dataLayer, unified_datalayer_product)
        scripts = doc.xpath('//script[contains(text(), "price")]')
        for script in scripts:
            script_text = script.text or ""
            # Look for dataLayer with price - match "price":9.99 or "price":"9.99"
            data_layer_match = re.search(r'"price"\s*:\s*["\']?(\d+[.,]\d{2}|\d+[.,]\d{1})', script_text, re.IGNORECASE)
            if data_layer_match:
                price_str = data_layer_match.group(1)
                price = self._parse_price(price_str)
                if price and 0 < price < 10000:
                    logger.debug(f"Found price from dataLayer: {price} (from {price_str})")
                    return price
            
            # Look for unified_datalayer_product with price
            unified_match = re.search(r'window\.unified_datalayer_product\s*=\s*\{[^}]*"price"\s*:\s*["\']?(\d+[.,]\d{2}|\d+[.,]\d{1})', script_text, re.IGNORECASE)
            if unified_match:
                price_str = unified_match.group(1)
                price = self._parse_price(price_str)
                if price and 0 < price < 10000:
                    logger.debug(f"Found price from unified_datalayer_product: {price} (from {price_str})")
                    return price
            
            # Look for JSON-like price in script - prefer decimal format
            json_price_match = re.search(r'["\']price["\']\s*:\s*["\']?(\d+[.,]\d{2}|\d+[.,]\d{1})', script_text, re.IGNORECASE)
            if json_price_match:
                price_str = json_price_match.group(1)
                price = self._parse_price(price_str)
                if price and 0 < price < 10000:
                    logger.debug(f"Found price from JSON in script: {price} (from {price_str})")
                    return price
        
        # Method 3: Try other data attributes
        selectors = [
            '//*[@data-product-price]',
            '//meta[@property="product:price:amount"]/@content',
            '//meta[@property="product:price"]/@content',
        ]
        
        for selector in selectors:
            try:
                elements = doc.xpath(selector)
                if elements:
                    if selector.endswith('/@content'):
                        price_str = elements[0]
                    else:
                        price_str = elements[0].get('data-product-price', '')
                    
                    if price_str:
                        price = self._parse_price(price_str)
                        if price and 0 < price < 10000:
                            logger.debug(f"Found price from {selector}: {price}")
                            return price
            except Exception:
                continue
        
        # Method 4: Look for price in CSS classes (but be careful with thresholds)
        price_elements = doc.xpath('//*[contains(@class, "price") and not(contains(@class, "threshold"))]')
        for elem in price_elements:
            text = elem.text_content()
            # Look for price patterns with decimal separator
            match = re.search(r'(\d+[,.]\d{2})\s*€?', text, re.IGNORECASE)
            if match:
                price = self._parse_price(match.group(1))
                if price and 0 < price < 10000:
                    logger.debug(f"Found price from price class element: {price}")
                    return price
        
        # Method 5: Fallback to text patterns (prioritize decimal separators)
        text = doc.text_content()
        price_patterns = [
            r'€\s*(\d+[,.]\d{2})\b',  # €9.99 or €9,99 (with word boundary)
            r'\b(\d+[,.]\d{2})\s*€',  # 9.99 € or 9,99€ (with word boundary)
            r'precio[:\s]+(\d+[,.]\d{2})\b',  # precio: 9.99
        ]
        
        for pattern in price_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                price = self._parse_price(match)
                if price and 0 < price < 10000:
                    logger.debug(f"Found price from text pattern {pattern}: {price}")
                    return price
        
        return None
    
    def _extract_description(self, doc: html.HtmlElement) -> Optional[str]:
        """Extract product description from HTML"""
        selectors = [
            '//*[@data-product-description]',
            '//*[contains(@class, "product-description")]',
            '//meta[@property="og:description"]/@content',
            '//meta[@name="description"]/@content',
        ]
        
        for selector in selectors:
            try:
                elements = doc.xpath(selector)
                if elements:
                    if selector.endswith('/@content'):
                        return elements[0].strip()
                    text = elements[0].text_content().strip()
                    if text:
                        return text
            except Exception:
                continue
        
        return None
    
    def _extract_brand(self, doc: html.HtmlElement) -> Optional[str]:
        """Extract brand from HTML"""
        selectors = [
            '//*[@data-brand]',
            '//*[contains(@class, "brand")]',
            '//meta[@property="product:brand"]/@content',
        ]
        
        for selector in selectors:
            try:
                elements = doc.xpath(selector)
                if elements:
                    if selector.endswith('/@content'):
                        return elements[0].strip()
                    text = elements[0].text_content().strip()
                    if text:
                        return text
            except Exception:
                continue
        
        return None
    
    def _extract_categories(self, doc: html.HtmlElement) -> List[str]:
        """Extract categories from HTML"""
        categories = []
        
        # Try breadcrumb navigation
        breadcrumb_selectors = [
            '//*[contains(@class, "breadcrumb")]//a',
            '//nav[contains(@class, "breadcrumb")]//a',
        ]
        
        for selector in breadcrumb_selectors:
            try:
                elements = doc.xpath(selector)
                for elem in elements:
                    text = elem.text_content().strip()
                    if text and text.lower() not in ['inicio', 'home', 'es']:
                        categories.append(text)
                if categories:
                    break
            except Exception:
                continue
        
        return categories
    
    def _extract_unit_price(self, doc: html.HtmlElement) -> Optional[Dict[str, str]]:
        """Extract unit price information from HTML"""
        # Look for unit price (price per kg, liter, etc.)
        selectors = [
            '//*[contains(@class, "unit-price")]',
            '//*[contains(@class, "price-per-unit")]',
        ]
        
        for selector in selectors:
            try:
                elements = doc.xpath(selector)
                if elements:
                    text = elements[0].text_content()
                    # Parse text like "1,23 €/kg" or "€1.23/kg"
                    match = re.search(r'([\d.,]+)\s*€\s*/?\s*(\w+)', text, re.IGNORECASE)
                    if match:
                        amount = self._parse_price(match.group(1))
                        unit = match.group(2).strip()
                        if amount:
                            return {"amount": amount, "unit": unit}
            except Exception:
                continue
        
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
                # Handle European format (comma as decimal separator)
                if ',' in clean_price and '.' in clean_price:
                    # If both, assume comma is decimal if it comes after dot
                    if clean_price.rindex(',') > clean_price.rindex('.'):
                        clean_price = clean_price.replace('.', '').replace(',', '.')
                    else:
                        clean_price = clean_price.replace(',', '')
                elif ',' in clean_price:
                    clean_price = clean_price.replace(',', '.')
                return Decimal(clean_price)
            elif isinstance(price_data, dict):
                # If price is an object with amount field
                amount = price_data.get("amount") or price_data.get("value")
                if amount is not None:
                    return self._parse_price(amount)
            
            return None
            
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not parse price {price_data}: {e}")
            return None
    
    async def test_connection(self) -> bool:
        """Test if we can connect to Lidl website"""
        try:
            # Try to fetch a single product to test connection
            test_product_id = 4989  # Use a known product ID from sitemap
            response = await self.make_request(f"{self.base_url}/es/p{test_product_id}")
            return response is not None and response.status_code in [200, 404]  # 404 is OK for non-existent product
            
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

