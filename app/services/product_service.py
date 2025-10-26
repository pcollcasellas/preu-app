from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session

from app.repositories.product_repository import ProductRepository
from app.repositories.price_history_repository import PriceHistoryRepository
from app.schemas import ProductFilters, ProductListResponse, Product, ProductWithHistory
from app.models import Products


class ProductService:
    """Service for product business logic"""
    
    def __init__(self, db: Session):
        self.db = db
        self.product_repo = ProductRepository(db)
        self.price_history_repo = PriceHistoryRepository(db)
    
    def get_product(self, product_id: int, supermarket_name: str) -> Optional[Product]:
        """Get a product by ID and supermarket"""
        product = self.product_repo.get_by_id(product_id, supermarket_name)
        if not product:
            return None
        return Product.model_validate(product)
    
    def get_products(self, filters: ProductFilters) -> ProductListResponse:
        """Get products with filters and pagination"""
        products, total = self.product_repo.get_all(filters)
        
        return ProductListResponse(
            products=[Product.model_validate(p) for p in products],
            total=total,
            page=filters.page,
            page_size=filters.page_size
        )
    
    def get_product_with_history(self, product_id: int, supermarket_name: str) -> Optional[ProductWithHistory]:
        """Get product with its price history"""
        product = self.product_repo.get_by_id(product_id, supermarket_name)
        if not product:
            return None
        
        history = self.price_history_repo.get_history(product_id, supermarket_name)
        
        return ProductWithHistory(
            **product.__dict__,
            price_history=history
        )
    
    def get_product_count(self, supermarket_name: Optional[str] = None) -> int:
        """Get total product count"""
        if supermarket_name:
            return self.product_repo.count_by_supermarket(supermarket_name)
        else:
            return self.product_repo.count_all()
    
    def get_products_by_supermarket(self, supermarket_name: str) -> List[Product]:
        """Get all products for a specific supermarket"""
        products = self.product_repo.get_by_supermarket(supermarket_name)
        return [Product.model_validate(p) for p in products]
    
    def upsert_product(self, product_data: Dict[str, Any]) -> Product:
        """Insert or update a product"""
        product = self.product_repo.upsert(product_data)
        return Product.model_validate(product)