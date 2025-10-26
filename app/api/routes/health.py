from datetime import datetime, timezone
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.database import get_db
from app.schemas import HealthCheck
from app.config import settings

router = APIRouter()


@router.get("/health", response_model=HealthCheck)
async def health_check(db: Session = Depends(get_db)):
    """Health check endpoint"""
    try:
        # Test database connection
        db.execute(text("SELECT 1"))
        database_connected = True
    except Exception as e:
        database_connected = False
    
    return HealthCheck(
        status="healthy" if database_connected else "unhealthy",
        timestamp=datetime.now(timezone.utc),
        database_connected=database_connected,
        environment=settings.environment
    )