import logging
from typing import Dict, Any, Optional
from datetime import datetime, timezone
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app.database import SessionLocal
from app.services.scraping_service import ScrapingService

logger = logging.getLogger(__name__)


class ScrapingScheduler:
    """Scheduler for managing scraping tasks - delegates all work to ScrapingService"""
    
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.is_running = False
        self.last_sitemap_refresh: Dict[str, datetime] = {}
    
    def start(self):
        """Start the scheduler"""
        if self.is_running:
            logger.warning("Scheduler is already running")
            return
        
        # Schedule daily sitemap refresh at 2 AM
        self.scheduler.add_job(
            self._refresh_all_sitemaps,
            CronTrigger(hour=2, minute=0),
            id="daily_sitemap_refresh",
            name="Daily Sitemap Refresh",
            replace_existing=True
        )
        
        # Schedule batch processing every 30 minutes
        self.scheduler.add_job(
            self._process_all_batches,
            IntervalTrigger(minutes=30),
            id="batch_processing",
            name="Batch Product Processing",
            replace_existing=True
        )
        
        self.scheduler.start()
        self.is_running = True
        logger.info("Scheduler started successfully")
    
    def stop(self):
        """Stop the scheduler"""
        if not self.is_running:
            return
        
        self.scheduler.shutdown()
        self.is_running = False
        logger.info("Scheduler stopped")
    
    async def _refresh_all_sitemaps(self):
        """Private method to refresh all sitemaps - delegates to ScrapingService"""
        logger.info("Scheduler: Starting daily sitemap refresh")
        
        db = SessionLocal()
        try:
            scraping_service = ScrapingService(db)
            results = await scraping_service.refresh_all_sitemaps()
            
            # Log results and track successful refreshes
            if results["success"]:
                logger.info(f"Scheduler: {results['message']}")
                for supermarket, result in results["results"].items():
                    logger.info(f"Scheduler: {supermarket} - {result['message']}")
                    # Record successful refresh timestamp
                    if result.get("success", False):
                        self.last_sitemap_refresh[supermarket] = datetime.now(timezone.utc)
            else:
                logger.error(f"Scheduler: {results['message']}")
                    
        except Exception as e:
            logger.error(f"Scheduler: Error in sitemap refresh: {e}")
        finally:
            db.close()
        
        logger.info("Scheduler: Daily sitemap refresh completed")
    
    async def _process_all_batches(self):
        """Private method to process all batches - delegates to ScrapingService"""
        logger.info("Scheduler: Starting batch processing")
        
        db = SessionLocal()
        try:
            scraping_service = ScrapingService(db)
            results = await scraping_service.process_all_batches()
            
            # Log results
            if results["success"]:
                logger.info(f"Scheduler: {results['message']}")
                for supermarket, result in results["results"].items():
                    logger.info(f"Scheduler: {supermarket} - {result['message']}")
            else:
                logger.error(f"Scheduler: {results['message']}")
                    
        except Exception as e:
            logger.error(f"Scheduler: Error in batch processing: {e}")
        finally:
            db.close()
        
        logger.info("Scheduler: Batch processing completed")
    
    def get_job_next_run(self, job_id: str) -> Optional[datetime]:
        """Get next run time for a specific job"""
        try:
            job = self.scheduler.get_job(job_id)
            return job.next_run_time if job else None
        except Exception:
            return None
    
    def get_last_sitemap_refresh(self, supermarket_name: str) -> Optional[datetime]:
        """Get last sitemap refresh time for a supermarket"""
        return self.last_sitemap_refresh.get(supermarket_name)
    
    def get_status(self) -> Dict[str, Any]:
        """Get current scheduler status"""
        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append({
                "id": job.id,
                "name": job.name,
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None
            })
        
        return {
            "is_running": self.is_running,
            "jobs": jobs
        }


# Global scheduler instance
scheduler = ScrapingScheduler()