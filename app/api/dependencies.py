from fastapi import Depends
from sqlalchemy.orm import Session

from app.database import get_db
from app.services.product_service import ProductService
from app.services.price_history_service import PriceHistoryService
from app.services.queue_service import QueueService
from app.services.scraping_service import ScrapingService


# Dependency injection functions for FastAPI
def get_product_service_dependency(db: Session = Depends(get_db)) -> ProductService:
    """FastAPI dependency for ProductService"""
    return ProductService(db)


def get_price_history_service_dependency(db: Session = Depends(get_db)) -> PriceHistoryService:
    """FastAPI dependency for PriceHistoryService"""
    return PriceHistoryService(db)


def get_queue_service_dependency(db: Session = Depends(get_db)) -> QueueService:
    """FastAPI dependency for QueueService"""
    return QueueService(db)


def get_scraping_service_dependency(db: Session = Depends(get_db)) -> ScrapingService:
    """FastAPI dependency for ScrapingService"""
    return ScrapingService(db)