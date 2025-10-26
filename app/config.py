from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # Database Configuration
    database_url: str
    
    # Environment
    environment: str = "dev"
    
    # Bonpreu Configuration
    bonpreu_base_url: str = "https://www.compraonline.bonpreuesclat.cat"
    bonpreu_sitemap_url: str = "https://www.compraonline.bonpreuesclat.cat/sitemaps/sitemap-products-part1.xml"
    
    # Mercadona Configuration
    mercadona_base_url: str = "https://tienda.mercadona.es"
    mercadona_sitemap_url: str = "https://tienda.mercadona.es/sitemap.xml"
    mercadona_api_url: str = "https://tienda.mercadona.es/api/products"
    
    # Scraping Configuration
    batch_size_fraction: float = 0.020833333  # 1/48
    batch_duration_minutes: int = 10
    concurrent_requests: int = 15
    request_timeout: int = 30
    
    # API Configuration
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Global settings instance
settings = Settings()
