from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_, func
from datetime import datetime, timezone, timedelta

from app.models import ProductPriceHistory


class PriceHistoryRepository:
    """Repository for price history data access operations"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def get_history(self, product_id: int, supermarket_name: str) -> List[ProductPriceHistory]:
        """Get price history for a product"""
        return self.db.query(ProductPriceHistory).filter(
            and_(
                ProductPriceHistory.product_id == product_id,
                ProductPriceHistory.supermarket_name == supermarket_name
            )
        ).order_by(ProductPriceHistory.valid_from.desc()).all()
    
    def get_current_price(self, product_id: int, supermarket_name: str) -> Optional[ProductPriceHistory]:
        """Get current price entry for a product"""
        return self.db.query(ProductPriceHistory).filter(
            and_(
                ProductPriceHistory.product_id == product_id,
                ProductPriceHistory.supermarket_name == supermarket_name,
                ProductPriceHistory.is_current == True
            )
        ).first()
    
    def create_entry(self, price_data: dict) -> ProductPriceHistory:
        """Create a new price history entry"""
        price_history = ProductPriceHistory(**price_data)
        self.db.add(price_history)
        return price_history
    
    def close_previous_entry(self, product_id: int, supermarket_name: str) -> int:
        """Set valid_to on the current price entry"""
        return self.db.query(ProductPriceHistory).filter(
            and_(
                ProductPriceHistory.product_id == product_id,
                ProductPriceHistory.supermarket_name == supermarket_name,
                ProductPriceHistory.is_current == True
            )
        ).update({
            "is_current": False,
            "valid_to": datetime.now(timezone.utc)
        })
    
    def get_price_changes(self, supermarket_name: str, days: int = 7) -> List[ProductPriceHistory]:
        """Get price changes in the last N days"""
        since_date = datetime.now(timezone.utc) - timedelta(days=days)
        
        return self.db.query(ProductPriceHistory).filter(
            and_(
                ProductPriceHistory.supermarket_name == supermarket_name,
                ProductPriceHistory.valid_from >= since_date
            )
        ).order_by(ProductPriceHistory.valid_from.desc()).all()
    
    def get_price_changes_count(self, supermarket_name: str, days: int = 7) -> int:
        """Get count of price changes in the last N days"""
        since_date = datetime.now(timezone.utc) - timedelta(days=days)
        
        return self.db.query(ProductPriceHistory).filter(
            and_(
                ProductPriceHistory.supermarket_name == supermarket_name,
                ProductPriceHistory.valid_from >= since_date
            )
        ).count()
    
    def get_products_with_price_changes(self, supermarket_name: str, days: int = 7) -> List[int]:
        """Get product IDs that had price changes in the last N days"""
        since_date = datetime.now(timezone.utc) - timedelta(days=days)
        
        results = self.db.query(ProductPriceHistory.product_id).filter(
            and_(
                ProductPriceHistory.supermarket_name == supermarket_name,
                ProductPriceHistory.valid_from >= since_date
            )
        ).distinct().all()
        
        return [result[0] for result in results]
    
    def get_price_trend(self, product_id: int, supermarket_name: str, days: int = 30) -> List[ProductPriceHistory]:
        """Get price trend for a product over the last N days"""
        since_date = datetime.now(timezone.utc) - timedelta(days=days)
        
        return self.db.query(ProductPriceHistory).filter(
            and_(
                ProductPriceHistory.product_id == product_id,
                ProductPriceHistory.supermarket_name == supermarket_name,
                ProductPriceHistory.valid_from >= since_date
            )
        ).order_by(ProductPriceHistory.valid_from.asc()).all()
    
    def delete_old_history(self, days_to_keep: int = 365) -> int:
        """Delete price history older than N days"""
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_to_keep)
        
        return self.db.query(ProductPriceHistory).filter(
            ProductPriceHistory.valid_from < cutoff_date
        ).delete()