from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session

from app.repositories.queue_repository import QueueRepository
from app.models import ProductScanQueue


class QueueService:
    """Service for scan queue business logic operations"""
    
    def __init__(self, db: Session):
        self.queue_repo = QueueRepository(db)
    
    def upsert_products_to_queue(self, supermarket_name: str, product_ids: List[int], priority: int = 0) -> int:
        """Add products to the scan queue with business logic"""
        if not product_ids:
            return 0
        
        if not supermarket_name:
            raise ValueError("Supermarket name is required")
        
        # Validate product IDs
        valid_product_ids = [pid for pid in product_ids if isinstance(pid, int) and pid > 0]
        
        if not valid_product_ids:
            raise ValueError("No valid product IDs provided")
        
        return self.queue_repo.upsert_many(valid_product_ids, supermarket_name, priority)
    
    def get_next_batch(self, supermarket_name: str, batch_size: int) -> List[ProductScanQueue]:
        """Get next batch of products to scan with business logic"""
        if batch_size <= 0:
            raise ValueError("Batch size must be greater than 0")
        
        if not supermarket_name:
            raise ValueError("Supermarket name is required")
        
        # Get more than needed to account for high priority items
        limit = batch_size * 2  # Get 2x to have options
        products = self.queue_repo.get_products_to_scan(supermarket_name, limit)
        
        # Prioritize high priority items (errors, never scanned)
        high_priority = []
        normal_priority = []
        
        for product in products:
            if product.error_count > 0 or product.last_scanned is None:
                high_priority.append(product)
            else:
                normal_priority.append(product)
        
        # Return high priority items first, then normal priority
        result = high_priority[:batch_size]
        if len(result) < batch_size:
            remaining = batch_size - len(result)
            result.extend(normal_priority[:remaining])
        
        return result
    
    def get_next_batch_with_metadata(self, supermarket_name: str) -> tuple[List[int], List[ProductScanQueue]]:
        """Get next batch of products with metadata for processing"""
        products = self.get_next_batch(supermarket_name, 1000)  # Get a reasonable batch size
        product_ids = [p.product_id for p in products]
        return product_ids, products
    
    def update_scan_results(self, products: List[ProductScanQueue], results: Dict[str, int]) -> None:
        """Update scan results for a batch of products"""
        if not products:
            return
        
        self.queue_repo.update_scan_results(products, results)
    
    def update_scan_status(self, product_id: int, supermarket_name: str, success: bool = True, error_message: str = None) -> bool:
        """Update scan status for a single product"""
        return self.queue_repo.update_scan_status(product_id, supermarket_name, success, error_message)
    
    def get_queue_statistics(self, supermarket_name: str) -> Dict[str, Any]:
        """Get queue statistics with business logic"""
        if not supermarket_name:
            raise ValueError("Supermarket name is required")
        
        stats = self.queue_repo.get_queue_stats(supermarket_name)
        
        # Add calculated fields
        if stats["total_products"] > 0:
            stats["scan_completion_rate"] = (stats["scanned_today"] / stats["total_products"]) * 100
            stats["error_rate"] = (stats["with_errors"] / stats["total_products"]) * 100
        else:
            stats["scan_completion_rate"] = 0.0
            stats["error_rate"] = 0.0
        
        return stats
    
    def get_high_priority_products(self, supermarket_name: str, limit: int = 100) -> List[ProductScanQueue]:
        """Get high priority products that need attention"""
        return self.queue_repo.get_high_priority_products(supermarket_name, limit)
    
    def reset_error_count(self, product_id: int, supermarket_name: str) -> bool:
        """Reset error count for a product (manual intervention)"""
        return self.queue_repo.reset_error_count(product_id, supermarket_name)
    
    def add_product_to_queue(self, product_id: int, supermarket_name: str, priority: int = 0) -> ProductScanQueue:
        """Add a single product to the queue"""
        if not product_id or product_id <= 0:
            raise ValueError("Valid product ID is required")
        
        if not supermarket_name:
            raise ValueError("Supermarket name is required")
        
        return self.queue_repo.upsert_product(product_id, supermarket_name, priority)
    
    def remove_product_from_queue(self, product_id: int, supermarket_name: str) -> bool:
        """Remove a product from the queue"""
        return self.queue_repo.delete_product(product_id, supermarket_name)
    
    def clear_queue(self, supermarket_name: str) -> int:
        """Clear all products from the queue for a supermarket"""
        if not supermarket_name:
            raise ValueError("Supermarket name is required")
        
        return self.queue_repo.clear_queue(supermarket_name)
    
    def get_products_never_scanned(self, supermarket_name: str, limit: int = 100) -> List[ProductScanQueue]:
        """Get products that have never been scanned"""
        all_products = self.queue_repo.get_products_to_scan(supermarket_name, limit * 2)
        return [p for p in all_products if p.last_scanned is None][:limit]
    
    def get_products_with_errors(self, supermarket_name: str, limit: int = 100) -> List[ProductScanQueue]:
        """Get products with scanning errors"""
        return self.queue_repo.get_high_priority_products(supermarket_name, limit)
    
    def prioritize_product(self, product_id: int, supermarket_name: str, priority_increase: int = 10) -> bool:
        """Increase priority for a specific product"""
        queue_item = self.queue_repo.get_products_to_scan(supermarket_name, 10000)  # Get all
        for item in queue_item:
            if item.product_id == product_id:
                item.scan_priority += priority_increase
                return True
        return False
    
    def get_queue_health_status(self, supermarket_name: str) -> Dict[str, Any]:
        """Get overall queue health status"""
        stats = self.get_queue_statistics(supermarket_name)
        
        # Determine health status
        if stats["error_rate"] > 20:
            health_status = "critical"
        elif stats["error_rate"] > 10:
            health_status = "warning"
        elif stats["scan_completion_rate"] < 50:
            health_status = "slow"
        else:
            health_status = "healthy"
        
        return {
            "status": health_status,
            "statistics": stats,
            "recommendations": self._get_health_recommendations(health_status, stats)
        }
    
    def _get_health_recommendations(self, status: str, stats: Dict[str, Any]) -> List[str]:
        """Get recommendations based on queue health status"""
        recommendations = []
        
        if status == "critical":
            recommendations.append("High error rate detected. Check scraper configuration and target website.")
            recommendations.append("Consider reducing concurrent requests or increasing delays.")
        
        if status == "warning":
            recommendations.append("Moderate error rate. Monitor closely and consider adjustments.")
        
        if status == "slow":
            recommendations.append("Low scan completion rate. Consider increasing batch size or frequency.")
        
        if stats["never_scanned"] > 0:
            recommendations.append(f"{stats['never_scanned']} products have never been scanned.")
        
        return recommendations