from sqlalchemy import Column, Integer, String, Boolean, Text, DateTime, Numeric, ForeignKey, Index, ForeignKeyConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import ARRAY
from datetime import datetime, timezone
from decimal import Decimal
from app.database import Base


def utc_now():
    """Helper function to get current UTC time with timezone awareness"""
    return datetime.now(timezone.utc)


class Products(Base):
    """Current product information table"""
    __tablename__ = "products"
    
    # Composite primary key
    product_id = Column(Integer, primary_key=True)
    supermarket_name = Column(String(50), primary_key=True)
    
    # Product details
    product_type = Column(String(100))
    product_name = Column(String(500), nullable=False)
    product_description = Column(Text)
    product_brand = Column(String(200))
    product_pack_size_description = Column(String(200))
    
    # Pricing
    product_price_amount = Column(Numeric(10, 2))
    product_currency = Column(String(3), default="EUR")
    product_unit_price_amount = Column(Numeric(10, 2))
    product_unit_price_currency = Column(String(3), default="EUR")
    product_unit_price_unit = Column(String(20))
    
    # Product attributes
    product_available = Column(Boolean, default=True)
    product_alcohol = Column(Boolean, default=False)
    product_cooking_guidelines = Column(Text)
    product_categories = Column(ARRAY(String))
    
    # Metadata
    last_updated = Column(DateTime, default=utc_now, onupdate=utc_now)
    created_at = Column(DateTime, default=utc_now)
    
    # Relationships
    price_history = relationship("ProductPriceHistory", back_populates="product")
    
    # Indexes
    __table_args__ = (
        Index('idx_products_supermarket', 'supermarket_name'),
        Index('idx_products_name', 'product_name'),
        Index('idx_products_brand', 'product_brand'),
        Index('idx_products_categories', 'product_categories', postgresql_using='gin'),
    )


class ProductPriceHistory(Base):
    """Slowly Changing Dimension Type 2 for price history"""
    __tablename__ = "product_price_history"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign key to products
    product_id = Column(Integer, nullable=False)
    supermarket_name = Column(String(50), nullable=False)
    
    # Price information
    product_price_amount = Column(Numeric(10, 2))
    product_unit_price_amount = Column(Numeric(10, 2))
    
    # SCD Type 2 fields
    valid_from = Column(DateTime, default=utc_now)
    valid_to = Column(DateTime)
    is_current = Column(Boolean, default=True)
    
    # Metadata
    created_at = Column(DateTime, default=utc_now)
    
    # Relationships
    product = relationship("Products", back_populates="price_history")
    
    # Indexes
    __table_args__ = (
        Index('idx_price_history_product', 'product_id', 'supermarket_name'),
        Index('idx_price_history_current', 'is_current'),
        Index('idx_price_history_valid_from', 'valid_from'),
        ForeignKeyConstraint(
            ['product_id', 'supermarket_name'],
            ['products.product_id', 'products.supermarket_name']
        ),
    )


class ProductScanQueue(Base):
    """Queue for managing which products to scan in each batch"""
    __tablename__ = "product_scan_queue"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Product reference
    product_id = Column(Integer, nullable=False)
    supermarket_name = Column(String(50), nullable=False)
    
    # Scan management
    last_scanned = Column(DateTime)
    scan_priority = Column(Integer, default=0)  # Higher number = higher priority
    scan_count = Column(Integer, default=0)  # How many times this product has been scanned
    last_error = Column(Text)  # Last error message if any
    error_count = Column(Integer, default=0)  # Number of consecutive errors
    
    # Metadata
    created_at = Column(DateTime, default=utc_now)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now)
    
    # Indexes
    __table_args__ = (
        Index('idx_scan_queue_supermarket', 'supermarket_name'),
        Index('idx_scan_queue_priority', 'scan_priority'),
        Index('idx_scan_queue_last_scanned', 'last_scanned'),
        Index('idx_scan_queue_error_count', 'error_count'),
        Index('idx_scan_queue_product', 'product_id', 'supermarket_name', unique=True),
    )
