from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func
from datetime import datetime, timezone, timedelta

from app.models import Products
from app.schemas import ProductFilters


class ProductRepository:
    """Repository for product data access operations"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def get_by_id(self, product_id: int, supermarket_name: str) -> Optional[Products]:
        """Get a product by ID and supermarket"""
        return self.db.query(Products).filter(
            and_(
                Products.product_id == product_id,
                Products.supermarket_name == supermarket_name
            )
        ).first()
    
    def get_all(self, filters: ProductFilters) -> tuple[List[Products], int]:
        """Get products with filters and pagination"""
        query = self.db.query(Products)
        
        # Apply filters
        if filters.supermarket_name:
            query = query.filter(Products.supermarket_name == filters.supermarket_name)
        
        if filters.product_name:
            query = query.filter(Products.product_name.ilike(f"%{filters.product_name}%"))
        
        if filters.product_brand:
            query = query.filter(Products.product_brand.ilike(f"%{filters.product_brand}%"))
        
        if filters.product_categories:
            # PostgreSQL array contains operator
            for category in filters.product_categories:
                query = query.filter(Products.product_categories.contains([category]))
        
        if filters.min_price is not None:
            query = query.filter(Products.product_price_amount >= filters.min_price)
        
        if filters.max_price is not None:
            query = query.filter(Products.product_price_amount <= filters.max_price)
        
        if filters.available_only:
            query = query.filter(Products.product_available == True)
        
        # Get total count
        total = query.count()
        
        # Apply pagination
        offset = (filters.page - 1) * filters.page_size
        products = query.offset(offset).limit(filters.page_size).all()
        
        return products, total
    
    def upsert(self, product_data: Dict[str, Any]) -> Products:
        """Insert or update a product"""
        existing_product = self.get_by_id(
            product_data["product_id"], 
            product_data["supermarket_name"]
        )
        
        if existing_product:
            # Update existing product
            for key, value in product_data.items():
                if key not in ["product_id", "supermarket_name"]:
                    setattr(existing_product, key, value)
            existing_product.last_updated = datetime.now(timezone.utc)
            return existing_product
        else:
            # Create new product
            new_product = Products(**product_data)
            self.db.add(new_product)
            return new_product
    
    def exists(self, product_id: int, supermarket_name: str) -> bool:
        """Check if a product exists"""
        return self.db.query(Products).filter(
            and_(
                Products.product_id == product_id,
                Products.supermarket_name == supermarket_name
            )
        ).first() is not None
    
    def get_by_supermarket(self, supermarket_name: str) -> List[Products]:
        """Get all products for a specific supermarket"""
        return self.db.query(Products).filter(
            Products.supermarket_name == supermarket_name
        ).all()
    
    def count_by_supermarket(self, supermarket_name: str) -> int:
        """Count products for a specific supermarket"""
        return self.db.query(Products).filter(
            Products.supermarket_name == supermarket_name
        ).count()
    
    def count_all(self) -> int:
        """Count all products"""
        return self.db.query(Products).count()
    
    def get_recently_updated(self, supermarket_name: str, hours: int = 24) -> List[Products]:
        """Get products updated in the last N hours"""
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        return self.db.query(Products).filter(
            and_(
                Products.supermarket_name == supermarket_name,
                Products.last_updated >= since
            )
        ).all()
    
    def delete(self, product_id: int, supermarket_name: str) -> bool:
        """Delete a product"""
        product = self.get_by_id(product_id, supermarket_name)
        if product:
            self.db.delete(product)
            return True
        return False