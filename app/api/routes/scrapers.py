from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.services.scraping_service import ScrapingService
from app.services.product_service import ProductService
from app.services.price_history_service import PriceHistoryService
from app.api.dependencies import (
    get_scraping_service_dependency,
    get_product_service_dependency,
    get_price_history_service_dependency
)
from app.schemas import ScrapingStatus, ScrapingResult

router = APIRouter()


@router.get("/scrapers/status", response_model=List[ScrapingStatus])
async def get_scraping_status(
    scraping_service: ScrapingService = Depends(get_scraping_service_dependency)
):
    """Get scraping status for all supermarkets"""
    from app.tasks.scheduler import scheduler
    
    statuses = []
    
    # Get all available scrapers dynamically
    for supermarket_name in scraping_service.scrapers.keys():
        status = scraping_service.get_scraping_status(scheduler)
        if supermarket_name in status:
            statuses.append(ScrapingStatus(**status[supermarket_name]))
    
    return statuses


@router.post("/scrapers/{supermarket_name}/refresh-products", response_model=ScrapingResult)
async def refresh_products(
    supermarket_name: str,
    background_tasks: BackgroundTasks,
    scraping_service: ScrapingService = Depends(get_scraping_service_dependency)
):
    """Trigger sitemap refresh for a supermarket"""
    # Add background task
    background_tasks.add_task(scraping_service.refresh_sitemap, supermarket_name)
    
    return ScrapingResult(
        success=True,
        message=f"Sitemap refresh started for {supermarket_name}",
        products_processed=0,
        products_updated=0,
        errors=0
    )


@router.post("/scrapers/{supermarket_name}/scan-batch", response_model=ScrapingResult)
async def scan_batch(
    supermarket_name: str,
    background_tasks: BackgroundTasks,
    scraping_service: ScrapingService = Depends(get_scraping_service_dependency)
):
    """Trigger manual batch scan for a supermarket"""
    # Add background task
    background_tasks.add_task(scraping_service.process_batch, supermarket_name)
    
    return ScrapingResult(
        success=True,
        message=f"Batch scan started for {supermarket_name}",
        products_processed=0,
        products_updated=0,
        errors=0
    )


@router.post("/scrapers/{supermarket_name}/products/{product_id}", response_model=ScrapingResult)
async def scan_single_product(
    supermarket_name: str,
    product_id: int,
    background_tasks: BackgroundTasks,
    scraping_service: ScrapingService = Depends(get_scraping_service_dependency)
):
    """Scan a single product"""
    # Add background task
    background_tasks.add_task(scraping_service.process_product, supermarket_name, product_id)
    
    return ScrapingResult(
        success=True,
        message=f"Product scan started for {product_id}",
        products_processed=1,
        products_updated=0,
        errors=0
    )


@router.get("/stats/products/count")
async def get_product_count(
    supermarket_name: Optional[str] = Query(None),
    product_service: ProductService = Depends(get_product_service_dependency)
):
    """Get total product count"""
    count = product_service.get_product_count(supermarket_name)
    return {"count": count}


@router.get("/stats/price-changes")
async def get_price_changes(
    supermarket_name: Optional[str] = Query(None),
    days: int = Query(7, description="Number of days to look back"),
    price_history_service: PriceHistoryService = Depends(get_price_history_service_dependency)
):
    """Get price changes in the last N days"""
    changes = price_history_service.get_price_changes(supermarket_name, days)
    
    return {
        "price_changes": changes,
        "days": days,
        "supermarket": supermarket_name
    }

