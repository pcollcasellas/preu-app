from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
from decimal import Decimal


# Product schemas
class ProductBase(BaseModel):
    product_id: int
    supermarket_name: str
    product_type: Optional[str] = None
    product_name: str
    product_description: Optional[str] = None
    product_brand: Optional[str] = None
    product_pack_size_description: Optional[str] = None
    product_price_amount: Optional[Decimal] = None
    product_currency: str = "EUR"
    product_unit_price_amount: Optional[Decimal] = None
    product_unit_price_currency: str = "EUR"
    product_unit_price_unit: Optional[str] = None
    product_available: bool = True
    product_alcohol: bool = False
    product_cooking_guidelines: Optional[str] = None
    product_categories: Optional[List[str]] = None


class ProductCreate(ProductBase):
    pass


class ProductUpdate(BaseModel):
    product_type: Optional[str] = None
    product_name: Optional[str] = None
    product_description: Optional[str] = None
    product_brand: Optional[str] = None
    product_pack_size_description: Optional[str] = None
    product_price_amount: Optional[Decimal] = None
    product_currency: Optional[str] = None
    product_unit_price_amount: Optional[Decimal] = None
    product_unit_price_currency: Optional[str] = None
    product_unit_price_unit: Optional[str] = None
    product_available: Optional[bool] = None
    product_alcohol: Optional[bool] = None
    product_cooking_guidelines: Optional[str] = None
    product_categories: Optional[List[str]] = None


class Product(ProductBase):
    last_updated: datetime
    created_at: datetime

    class Config:
        from_attributes = True


# Price history schemas
class ProductPriceHistoryBase(BaseModel):
    product_id: int
    supermarket_name: str
    product_price_amount: Optional[Decimal] = None
    product_unit_price_amount: Optional[Decimal] = None


class ProductPriceHistoryCreate(ProductPriceHistoryBase):
    pass


class ProductPriceHistory(ProductPriceHistoryBase):
    id: int
    valid_from: datetime
    valid_to: Optional[datetime] = None
    is_current: bool
    created_at: datetime

    class Config:
        from_attributes = True


# Scan queue schemas
class ProductScanQueueBase(BaseModel):
    product_id: int
    supermarket_name: str
    scan_priority: int = 0


class ProductScanQueueCreate(ProductScanQueueBase):
    pass


class ProductScanQueue(ProductScanQueueBase):
    id: int
    last_scanned: Optional[datetime] = None
    scan_count: int = 0
    last_error: Optional[str] = None
    error_count: int = 0
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# API response schemas
class ProductWithHistory(Product):
    price_history: List[ProductPriceHistory] = []


class ProductListResponse(BaseModel):
    products: List[Product]
    total: int
    page: int
    page_size: int


class ScrapingStatus(BaseModel):
    supermarket_name: str
    total_products: int
    products_scanned_today: int
    last_sitemap_refresh: Optional[datetime] = None
    next_batch_scan: Optional[datetime] = None
    is_running: bool = False


class ScrapingResult(BaseModel):
    success: bool
    message: str
    products_processed: int = 0
    products_updated: int = 0
    errors: int = 0


# Query parameters
class ProductFilters(BaseModel):
    supermarket_name: Optional[str] = None
    product_name: Optional[str] = None
    product_brand: Optional[str] = None
    product_categories: Optional[List[str]] = None
    min_price: Optional[Decimal] = None
    max_price: Optional[Decimal] = None
    available_only: bool = True
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=100)


class HealthCheck(BaseModel):
    status: str
    timestamp: datetime
    database_connected: bool
    environment: str
