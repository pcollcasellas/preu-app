from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func
from datetime import datetime, timezone

from app.models import ProductScanQueue


class QueueRepository:
    """Repository for scan queue data access operations"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def get_products_to_scan(self, supermarket_name: str, limit: int = 1000) -> List[ProductScanQueue]:
        """Get products to scan, ordered by priority and last scanned"""
        return self.db.query(ProductScanQueue).filter(
            ProductScanQueue.supermarket_name == supermarket_name
        ).order_by(
            ProductScanQueue.scan_priority.desc(),
            ProductScanQueue.last_scanned.asc().nullsfirst()
        ).limit(limit).all()
    
    def upsert_product(self, product_id: int, supermarket_name: str, priority: int = 0) -> ProductScanQueue:
        """Add or update a single product in the queue"""
        existing = self.db.query(ProductScanQueue).filter(
            and_(
                ProductScanQueue.product_id == product_id,
                ProductScanQueue.supermarket_name == supermarket_name
            )
        ).first()
        
        if existing:
            existing.updated_at = datetime.now(timezone.utc)
            existing.scan_priority = max(existing.scan_priority, priority)
            return existing
        else:
            new_item = ProductScanQueue(
                product_id=product_id,
                supermarket_name=supermarket_name,
                scan_priority=priority
            )
            self.db.add(new_item)
            return new_item
    
    def upsert_many(self, product_ids: List[int], supermarket_name: str, priority: int = 0) -> int:
        """Bulk upsert products to the queue using efficient bulk operations"""
        if not product_ids:
            return 0
        
        # Step 1: Fetch all existing products in ONE query
        existing_products = self.db.query(ProductScanQueue).filter(
            and_(
                ProductScanQueue.product_id.in_(product_ids),
                ProductScanQueue.supermarket_name == supermarket_name
            )
        ).all()
        
        # Step 2: Create lookup set for existing product IDs
        existing_product_ids = {p.product_id for p in existing_products}
        
        # Step 3: Separate new vs existing products
        new_product_ids = [pid for pid in product_ids if pid not in existing_product_ids]
        existing_products_dict = {p.product_id: p for p in existing_products}
        
        # Step 4: Bulk insert new products
        if new_product_ids:
            new_items = [
                {
                    'product_id': product_id,
                    'supermarket_name': supermarket_name,
                    'scan_priority': priority,
                    'created_at': datetime.now(timezone.utc),
                    'updated_at': datetime.now(timezone.utc)
                }
                for product_id in new_product_ids
            ]
            self.db.bulk_insert_mappings(ProductScanQueue, new_items)
        
        # Step 5: Bulk update existing products
        if existing_products:
            update_items = []
            for product in existing_products:
                if product.scan_priority < priority:
                    update_items.append({
                        'id': product.id,
                        'scan_priority': priority,
                        'updated_at': datetime.now(timezone.utc)
                    })
            
            if update_items:
                self.db.bulk_update_mappings(ProductScanQueue, update_items)
        
        return len(new_product_ids)
    
    def update_scan_status(self, product_id: int, supermarket_name: str, success: bool = True, error_message: str = None) -> bool:
        """Update scan status for a product"""
        queue_item = self.db.query(ProductScanQueue).filter(
            and_(
                ProductScanQueue.product_id == product_id,
                ProductScanQueue.supermarket_name == supermarket_name
            )
        ).first()
        
        if queue_item:
            queue_item.last_scanned = datetime.now(timezone.utc)
            queue_item.scan_count += 1
            queue_item.updated_at = datetime.now(timezone.utc)
            
            if success:
                queue_item.error_count = 0
                queue_item.last_error = None
            else:
                queue_item.error_count += 1
                queue_item.last_error = error_message
                # Increase priority for failed items to retry sooner
                queue_item.scan_priority += 1
            
            return True
        return False
    
    def update_scan_results(self, products: List[ProductScanQueue], results: Dict[str, int]) -> None:
        """Update scan results for a batch of products"""
        for product in products:
            product.last_scanned = datetime.now(timezone.utc)
            product.scan_count += 1
            product.updated_at = datetime.now(timezone.utc)
    
    def get_queue_stats(self, supermarket_name: str) -> Dict[str, Any]:
        """Get queue statistics for a supermarket"""
        total = self.db.query(ProductScanQueue).filter(
            ProductScanQueue.supermarket_name == supermarket_name
        ).count()
        
        scanned_today = self.db.query(ProductScanQueue).filter(
            and_(
                ProductScanQueue.supermarket_name == supermarket_name,
                func.date(ProductScanQueue.last_scanned) == datetime.now(timezone.utc).date()
            )
        ).count()
        
        with_errors = self.db.query(ProductScanQueue).filter(
            and_(
                ProductScanQueue.supermarket_name == supermarket_name,
                ProductScanQueue.error_count > 0
            )
        ).count()
        
        never_scanned = self.db.query(ProductScanQueue).filter(
            and_(
                ProductScanQueue.supermarket_name == supermarket_name,
                ProductScanQueue.last_scanned.is_(None)
            )
        ).count()
        
        return {
            "total_products": total,
            "scanned_today": scanned_today,
            "with_errors": with_errors,
            "never_scanned": never_scanned
        }
    
    def get_high_priority_products(self, supermarket_name: str, limit: int = 100) -> List[ProductScanQueue]:
        """Get high priority products (with errors or never scanned)"""
        return self.db.query(ProductScanQueue).filter(
            and_(
                ProductScanQueue.supermarket_name == supermarket_name,
                or_(
                    ProductScanQueue.error_count > 0,
                    ProductScanQueue.last_scanned.is_(None)
                )
            )
        ).order_by(
            ProductScanQueue.scan_priority.desc(),
            ProductScanQueue.error_count.desc()
        ).limit(limit).all()
    
    def reset_error_count(self, product_id: int, supermarket_name: str) -> bool:
        """Reset error count for a product"""
        queue_item = self.db.query(ProductScanQueue).filter(
            and_(
                ProductScanQueue.product_id == product_id,
                ProductScanQueue.supermarket_name == supermarket_name
            )
        ).first()
        
        if queue_item:
            queue_item.error_count = 0
            queue_item.last_error = None
            queue_item.updated_at = datetime.now(timezone.utc)
            return True
        return False
    
    def delete_product(self, product_id: int, supermarket_name: str) -> bool:
        """Remove a product from the queue"""
        queue_item = self.db.query(ProductScanQueue).filter(
            and_(
                ProductScanQueue.product_id == product_id,
                ProductScanQueue.supermarket_name == supermarket_name
            )
        ).first()
        
        if queue_item:
            self.db.delete(queue_item)
            return True
        return False
    
    def clear_queue(self, supermarket_name: str) -> int:
        """Clear all products from the queue for a supermarket"""
        return self.db.query(ProductScanQueue).filter(
            ProductScanQueue.supermarket_name == supermarket_name
        ).delete()