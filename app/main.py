import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import health, products, scrapers
from app.tasks.scheduler import scheduler
from app.config import settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Suppress httpx HTTP client logs to keep progress bar clean
logging.getLogger("httpx").setLevel(logging.WARNING)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan event handler for startup and shutdown"""
    # Startup
    scheduler.start()
    logger.info("Application started")
    yield
    # Shutdown
    scheduler.stop()
    logger.info("Application stopped")


# Create FastAPI app
app = FastAPI(
    title="Supermarket Price Scraper API",
    description="API for scraping and comparing supermarket prices",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(health.router, prefix="/api", tags=["health"])
app.include_router(products.router, prefix="/api", tags=["products"])
app.include_router(scrapers.router, prefix="/api", tags=["scrapers"])


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.environment == "dev"
    )