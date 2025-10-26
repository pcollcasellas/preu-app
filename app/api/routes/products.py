from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.services.product_service import ProductService
from app.services.price_history_service import PriceHistoryService
from app.api.dependencies import get_product_service_dependency, get_price_history_service_dependency
from app.schemas import (
    Product, ProductWithHistory, ProductListResponse, ProductFilters,
    ProductPriceHistory
)

router = APIRouter()


@router.get("/products", response_model=ProductListResponse)
async def get_products(
    filters: ProductFilters = Depends(),
    product_service: ProductService = Depends(get_product_service_dependency)
):
    """Get products with optional filters"""
    return product_service.get_products(filters)


@router.get("/products/{product_id}", response_model=Product)
async def get_product(
    product_id: int,
    supermarket_name: str = Query(..., description="Supermarket name"),
    product_service: ProductService = Depends(get_product_service_dependency)
):
    """Get a specific product by ID and supermarket"""
    product = product_service.get_product(product_id, supermarket_name)
    
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    return product


@router.get("/products/{product_id}/history", response_model=List[ProductPriceHistory])
async def get_product_history(
    product_id: int,
    supermarket_name: str = Query(..., description="Supermarket name"),
    price_history_service: PriceHistoryService = Depends(get_price_history_service_dependency)
):
    """Get price history for a specific product"""
    history = price_history_service.get_price_history(product_id, supermarket_name)
    return history


@router.get("/products/{product_id}/full", response_model=ProductWithHistory)
async def get_product_with_history(
    product_id: int,
    supermarket_name: str = Query(..., description="Supermarket name"),
    product_service: ProductService = Depends(get_product_service_dependency)
):
    """Get product with its price history"""
    product_with_history = product_service.get_product_with_history(product_id, supermarket_name)
    
    if not product_with_history:
        raise HTTPException(status_code=404, detail="Product not found")
    
    return product_with_history


@router.get("/products/search/{query}")
async def search_products(
    query: str,
    supermarket_name: Optional[str] = Query(None, description="Filter by supermarket"),
    product_service: ProductService = Depends(get_product_service_dependency)
):
    """Search products by name or brand"""
    products = product_service.search_products(query, supermarket_name)
    return {"products": products, "query": query, "count": len(products)}


@router.get("/products/category/{category}")
async def get_products_by_category(
    category: str,
    supermarket_name: Optional[str] = Query(None, description="Filter by supermarket"),
    product_service: ProductService = Depends(get_product_service_dependency)
):
    """Get products by category"""
    products = product_service.get_products_by_category(category, supermarket_name)
    return {"products": products, "category": category, "count": len(products)}


@router.get("/products/price-range")
async def get_products_by_price_range(
    min_price: float = Query(..., description="Minimum price"),
    max_price: float = Query(..., description="Maximum price"),
    supermarket_name: Optional[str] = Query(None, description="Filter by supermarket"),
    product_service: ProductService = Depends(get_product_service_dependency)
):
    """Get products within a price range"""
    products = product_service.get_products_by_price_range(min_price, max_price, supermarket_name)
    return {
        "products": products,
        "min_price": min_price,
        "max_price": max_price,
        "count": len(products)
    }