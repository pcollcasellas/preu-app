import logging
from typing import Dict, Any, List
from sqlalchemy.orm import Session
from tqdm.asyncio import tqdm as atqdm

from app.repositories.product_repository import ProductRepository
from app.repositories.price_history_repository import PriceHistoryRepository
from app.repositories.queue_repository import QueueRepository
from app.services.product_service import ProductService
from app.services.price_history_service import PriceHistoryService
from app.services.queue_service import QueueService
from app.scrapers.base import BaseScraper
from app.scrapers.bonpreu import BonpreuScraper
from app.scrapers.mercadona import MercadonaScraper
from app.config import settings
# Note: scheduler is passed as parameter to avoid circular import

logger = logging.getLogger(__name__)


class ScrapingService:
    """Main service for orchestrating scraping workflows"""
    
    def __init__(self, db: Session):
        self.db = db
        
        # Initialize repositories
        self.product_repo = ProductRepository(db)
        self.price_history_repo = PriceHistoryRepository(db)
        self.queue_repo = QueueRepository(db)
        
        # Initialize services
        self.product_service = ProductService(db)
        self.price_history_service = PriceHistoryService(db)
        self.queue_service = QueueService(db)
        
        # Available scrapers
        self.scrapers = {
            "bonpreu": BonpreuScraper,
            "mercadona": MercadonaScraper
        }
    
    async def refresh_sitemap(self, supermarket_name: str) -> Dict[str, Any]:
        """Refresh sitemap for a supermarket and update the queue"""
        if supermarket_name not in self.scrapers:
            raise ValueError(f"Unknown supermarket: {supermarket_name}")
        
        logger.info(f"Starting sitemap refresh for {supermarket_name}")
        
        scraper_class = self.scrapers[supermarket_name]
        
        try:
            async with scraper_class() as scraper:
                # Fetch all product IDs from sitemap
                product_ids = await scraper.fetch_sitemap_products()
                
                if not product_ids:
                    logger.warning(f"No products found in sitemap for {supermarket_name}")
                    return {
                        "success": False,
                        "message": "No products found in sitemap",
                        "products_processed": 0
                    }
                
                # Update scan queue with new products
                added_count = self.queue_service.upsert_products_to_queue(
                    supermarket_name, product_ids
                )
                
                # Commit the transaction to persist changes
                self.db.commit()
                
                logger.info(f"Updated scan queue for {supermarket_name} with {len(product_ids)} products ({added_count} new)")
                
                return {
                    "success": True,
                    "message": f"Sitemap refreshed successfully",
                    "products_processed": len(product_ids),
                    "new_products_added": added_count
                }
                
        except Exception as e:
            logger.error(f"Error refreshing sitemap for {supermarket_name}: {e}")
            # Rollback the transaction on error
            self.db.rollback()
            return {
                "success": False,
                "message": str(e),
                "products_processed": 0
            }
    
    async def process_batch(self, supermarket_name: str) -> Dict[str, Any]:
        """Process a batch of products for a supermarket"""
        if supermarket_name not in self.scrapers:
            raise ValueError(f"Unknown supermarket: {supermarket_name}")
        
        logger.info(f"Starting batch processing for {supermarket_name}")
        
        try:
            # Get products to scan
            product_ids, products_to_scan = self.queue_service.get_next_batch_with_metadata(supermarket_name)
            
            if not product_ids:
                logger.info(f"No products to scan for {supermarket_name}")
                return {
                    "success": True,
                    "message": "No products to scan",
                    "products_processed": 0,
                    "products_updated": 0,
                    "errors": 0
                }
            
            logger.info(f"Processing {len(product_ids)} products for {supermarket_name}")
            
            # Process products with scraper
            scraper_class = self.scrapers[supermarket_name]
            async with scraper_class() as scraper:
                # Update products in database
                updated_count = 0
                error_count = 0
                
                # Create progress bar with tqdm
                pbar = atqdm(
                    product_ids,
                    desc=f"Scraping {supermarket_name}",
                    unit="product",
                    ncols=100,
                    bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
                )
                
                for product_id in pbar:
                    try:
                        # Update progress bar with success/error counts
                        pbar.set_postfix({'✅': updated_count, '❌': error_count}, refresh=False)
                        
                        # Fetch product details
                        product_data = await scraper.fetch_product_details(product_id)
                        
                        if product_data:
                            # Check if product exists
                            existing_product = self.product_repo.get_by_id(
                                product_id, supermarket_name
                            )
                            
                            # Check if price changed
                            price_changed = self.price_history_service.has_price_changed(
                                existing_product, product_data
                            )
                            
                            # Create price history if price changed
                            if price_changed:
                                self.price_history_service.create_price_history_entry(
                                    product_data, existing_product
                                )
                            
                            # Update or create product
                            self.product_service.upsert_product(product_data)
                            updated_count += 1
                            
                            # Update scan status
                            self.queue_service.update_scan_status(
                                product_id, supermarket_name, success=True
                            )
                        else:
                            error_count += 1
                            self.queue_service.update_scan_status(
                                product_id, supermarket_name, success=False, 
                                error_message="Product not found"
                            )
                            
                    except Exception as e:
                        pbar.write(f"ERROR: Product {product_id}: {e}")
                        error_count += 1
                        self.queue_service.update_scan_status(
                            product_id, supermarket_name, success=False,
                            error_message=str(e)
                        )
                
                # Update scan queue with results
                self.queue_service.update_scan_results(products_to_scan, {
                    "processed": len(product_ids),
                    "updated": updated_count,
                    "errors": error_count
                })
                
                # Close progress bar before final summary
                pbar.close()
                
                # Print final summary
                logger.info(f"✅ Batch processing completed for {supermarket_name}")
                logger.info(f"   📊 Total processed: {len(product_ids)}")
                logger.info(f"   ✅ Successfully updated: {updated_count}")
                logger.info(f"   ❌ Errors: {error_count}")
                logger.info(f"   📈 Success rate: {(updated_count / len(product_ids) * 100):.1f}%")
                
                # Commit all changes
                self.db.commit()
                
                return {
                    "success": True,
                    "message": "Batch processing completed",
                    "products_processed": len(product_ids),
                    "products_updated": updated_count,
                    "errors": error_count
                }
                
        except Exception as e:
            logger.error(f"Error processing batch for {supermarket_name}: {e}")
            
            # Rollback any changes
            self.db.rollback()
            
            return {
                "success": False,
                "message": str(e),
                "products_processed": 0,
                "products_updated": 0,
                "errors": 0
            }
    
    async def process_product(self, supermarket_name: str, product_id: int) -> Dict[str, Any]:
        """Process a single product"""
        if supermarket_name not in self.scrapers:
            raise ValueError(f"Unknown supermarket: {supermarket_name}")
        
        logger.info(f"Processing single product {product_id} for {supermarket_name}")
        
        try:
            scraper_class = self.scrapers[supermarket_name]
            async with scraper_class() as scraper:
                # Fetch product details
                product_data = await scraper.fetch_product_details(product_id)
                
                if not product_data:
                    return {
                        "success": False,
                        "message": "Product not found",
                        "product_id": product_id
                    }
                
                # Check if product exists
                existing_product = self.product_repo.get_by_id(product_id, supermarket_name)
                
                # Check if price changed
                price_changed = self.price_history_service.has_price_changed(
                    existing_product, product_data
                )
                
                # Create price history if price changed
                if price_changed:
                    self.price_history_service.create_price_history_entry(
                        product_data, existing_product
                    )
                
                # Update or create product
                updated_product = self.product_service.upsert_product(product_data)
                
                # Update scan status
                self.queue_service.update_scan_status(
                    product_id, supermarket_name, success=True
                )
                
                # Commit all changes
                self.db.commit()
                
                return {
                    "success": True,
                    "message": "Product updated successfully",
                    "product_id": product_id,
                    "price_changed": price_changed
                }
                
        except Exception as e:
            logger.error(f"Error processing product {product_id}: {e}")
            
            # Update scan status with error
            self.queue_service.update_scan_status(
                product_id, supermarket_name, success=False,
                error_message=str(e)
            )
            
            # Rollback any changes on error
            self.db.rollback()
            
            return {
                "success": False,
                "message": str(e),
                "product_id": product_id
            }
    
    async def refresh_all_sitemaps(self) -> Dict[str, Any]:
        """Refresh sitemaps for all supermarkets"""
        logger.info("Starting refresh of all sitemaps")
        
        results = {}
        total_processed = 0
        
        for supermarket_name in self.scrapers.keys():
            try:
                result = await self.refresh_sitemap(supermarket_name)
                results[supermarket_name] = result
                total_processed += result.get("products_processed", 0)
            except Exception as e:
                logger.error(f"Error refreshing sitemap for {supermarket_name}: {e}")
                results[supermarket_name] = {
                    "success": False,
                    "message": str(e),
                    "products_processed": 0
                }
        
        logger.info(f"All sitemaps refresh completed. Total products processed: {total_processed}")
        
        return {
            "success": True,
            "message": "All sitemaps refreshed",
            "total_products_processed": total_processed,
            "results": results
        }
    
    async def process_all_batches(self) -> Dict[str, Any]:
        """Process batches for all supermarkets"""
        logger.info("Starting batch processing for all supermarkets")
        
        results = {}
        total_processed = 0
        total_updated = 0
        total_errors = 0
        
        for supermarket_name in self.scrapers.keys():
            try:
                result = await self.process_batch(supermarket_name)
                results[supermarket_name] = result
                total_processed += result.get("products_processed", 0)
                total_updated += result.get("products_updated", 0)
                total_errors += result.get("errors", 0)
            except Exception as e:
                logger.error(f"Error processing batch for {supermarket_name}: {e}")
                results[supermarket_name] = {
                    "success": False,
                    "message": str(e),
                    "products_processed": 0,
                    "products_updated": 0,
                    "errors": 0
                }
        
        logger.info(f"All batches processing completed. Processed: {total_processed}, Updated: {total_updated}, Errors: {total_errors}")
        
        return {
            "success": True,
            "message": "All batches processed",
            "total_products_processed": total_processed,
            "total_products_updated": total_updated,
            "total_errors": total_errors,
            "results": results
        }
    
    def get_scraping_status(self, scheduler=None) -> Dict[str, Any]:
        """Get scraping status for all supermarkets"""
        statuses = {}
        
        for supermarket_name in self.scrapers.keys():
            try:
                queue_stats = self.queue_service.get_queue_statistics(supermarket_name)
                product_count = self.product_service.get_product_count(supermarket_name)
                
                # Get scheduler information if available
                if scheduler:
                    is_running = scheduler.is_running
                    next_batch_scan = scheduler.get_job_next_run("batch_processing")
                    last_sitemap_refresh = scheduler.get_last_sitemap_refresh(supermarket_name)
                else:
                    is_running = False
                    next_batch_scan = None
                    last_sitemap_refresh = None
                
                statuses[supermarket_name] = {
                    "supermarket_name": supermarket_name,
                    "total_products": product_count,
                    "products_scanned_today": queue_stats["scanned_today"],
                    "last_sitemap_refresh": last_sitemap_refresh,
                    "next_batch_scan": next_batch_scan,
                    "is_running": is_running
                }
            except Exception as e:
                logger.error(f"Error getting status for {supermarket_name}: {e}")
                statuses[supermarket_name] = {
                    "supermarket_name": supermarket_name,
                    "total_products": 0,
                    "products_scanned_today": 0,
                    "last_sitemap_refresh": None,
                    "next_batch_scan": None,
                    "is_running": False,
                    "error": str(e)
                }
        
        return statuses