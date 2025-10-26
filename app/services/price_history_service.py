from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from datetime import datetime, timezone
from decimal import Decimal

from app.repositories.price_history_repository import PriceHistoryRepository
from app.models import Products, ProductPriceHistory


class PriceHistoryService:
    """Service for price history business logic operations"""
    
    def __init__(self, db: Session):
        self.price_history_repo = PriceHistoryRepository(db)
    
    def has_price_changed(self, old_product: Optional[Products], new_data: Dict[str, Any]) -> bool:
        """Detect if price has changed between old and new product data"""
        if not old_product:
            # New product, so we need to create price history
            return True
        
        old_price = old_product.product_price_amount
        old_unit_price = old_product.product_unit_price_amount
        new_price = new_data.get("product_price_amount")
        new_unit_price = new_data.get("product_unit_price_amount")
        
        # Compare prices (handle None values)
        price_changed = False
        
        if old_price != new_price:
            price_changed = True
        elif old_unit_price != new_unit_price:
            price_changed = True
        
        return price_changed
    
    def create_price_history_entry(self, product_data: Dict[str, Any], old_product: Optional[Products] = None) -> ProductPriceHistory:
        """Create a new price history entry with SCD Type 2 logic"""
        # Close previous entry if it exists
        if old_product:
            self.price_history_repo.close_previous_entry(
                product_data["product_id"],
                product_data["supermarket_name"]
            )
        
        # Create new price history entry
        price_history_data = {
            "product_id": product_data["product_id"],
            "supermarket_name": product_data["supermarket_name"],
            "product_price_amount": product_data.get("product_price_amount"),
            "product_unit_price_amount": product_data.get("product_unit_price_amount"),
            "valid_from": datetime.now(timezone.utc),
            "is_current": True
        }
        
        return self.price_history_repo.create_entry(price_history_data)
    
    def get_price_history(self, product_id: int, supermarket_name: str) -> List[ProductPriceHistory]:
        """Get price history for a product"""
        return self.price_history_repo.get_history(product_id, supermarket_name)
    
    def get_current_price(self, product_id: int, supermarket_name: str) -> Optional[ProductPriceHistory]:
        """Get current price entry for a product"""
        return self.price_history_repo.get_current_price(product_id, supermarket_name)
    
    def get_price_changes(self, supermarket_name: str, days: int = 7) -> List[ProductPriceHistory]:
        """Get price changes in the last N days"""
        return self.price_history_repo.get_price_changes(supermarket_name, days)
    
    def get_price_changes_count(self, supermarket_name: str, days: int = 7) -> int:
        """Get count of price changes in the last N days"""
        return self.price_history_repo.get_price_changes_count(supermarket_name, days)
    
    def get_products_with_price_changes(self, supermarket_name: str, days: int = 7) -> List[int]:
        """Get product IDs that had price changes in the last N days"""
        return self.price_history_repo.get_products_with_price_changes(supermarket_name, days)
    
    def get_price_trend(self, product_id: int, supermarket_name: str, days: int = 30) -> List[ProductPriceHistory]:
        """Get price trend for a product over the last N days"""
        return self.price_history_repo.get_price_trend(product_id, supermarket_name, days)
    
    def calculate_price_change_percentage(self, product_id: int, supermarket_name: str, days: int = 30) -> Optional[float]:
        """Calculate price change percentage over the last N days"""
        trend = self.get_price_trend(product_id, supermarket_name, days)
        
        if len(trend) < 2:
            return None
        
        oldest_price = trend[0].product_price_amount
        newest_price = trend[-1].product_price_amount
        
        if not oldest_price or not newest_price:
            return None
        
        if oldest_price == 0:
            return None
        
        change_percentage = ((newest_price - oldest_price) / oldest_price) * 100
        return float(change_percentage)
    
    def get_average_price(self, product_id: int, supermarket_name: str, days: int = 30) -> Optional[Decimal]:
        """Get average price for a product over the last N days"""
        trend = self.get_price_trend(product_id, supermarket_name, days)
        
        if not trend:
            return None
        
        prices = [entry.product_price_amount for entry in trend if entry.product_price_amount is not None]
        
        if not prices:
            return None
        
        return sum(prices) / len(prices)
    
    def get_price_statistics(self, supermarket_name: str, days: int = 7) -> Dict[str, Any]:
        """Get price change statistics for a supermarket"""
        changes = self.get_price_changes(supermarket_name, days)
        
        if not changes:
            return {
                "total_changes": 0,
                "price_increases": 0,
                "price_decreases": 0,
                "average_change_percentage": 0.0
            }
        
        increases = 0
        decreases = 0
        total_change_percentage = 0.0
        valid_changes = 0
        
        for change in changes:
            # This is a simplified calculation - in reality, you'd need to compare
            # with the previous price entry to determine if it's an increase or decrease
            if change.product_price_amount:
                # For now, we'll just count all changes
                # A more sophisticated implementation would compare with previous entry
                valid_changes += 1
        
        return {
            "total_changes": len(changes),
            "price_increases": increases,
            "price_decreases": decreases,
            "average_change_percentage": total_change_percentage / valid_changes if valid_changes > 0 else 0.0
        }
    
    def cleanup_old_history(self, days_to_keep: int = 365) -> int:
        """Delete price history older than N days"""
        return self.price_history_repo.delete_old_history(days_to_keep)