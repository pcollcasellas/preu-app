# Supermarket Price Scraper API

A FastAPI application for scraping and comparing supermarket prices, starting with Bonpreu.

## Features

- **Asynchronous scraping** with rate limiting to avoid being blocked
- **Daily sitemap refresh** to discover new products
- **Batch processing** every 30 minutes to update prices
- **Price history tracking** using Slowly Changing Dimension (SCD) Type 2
- **RESTful API** for querying products and price data
- **PostgreSQL database** with Neon support

## Architecture

### Database Models

- **Products**: Current product information
- **ProductPriceHistory**: Historical price data (SCD Type 2)
- **ProductScanQueue**: Queue management for batch processing

### Scraping Strategy

1. **Daily (2 AM)**: Refresh product list from sitemap
2. **Every 30 minutes**: Process 1/48th of total products
3. **Rate limiting**: Spread requests over 10 minutes with concurrent batches
4. **Error handling**: Retry logic with exponential backoff

## Setup

### Prerequisites

- Python 3.11+
- PostgreSQL database (Neon recommended)
- UV package manager

### Installation

1. Clone the repository
2. Install dependencies with UV:
   ```bash
   uv sync
   ```

3. Create environment file:
   ```bash
   cp .env.example .env
   ```

4. Configure your database URL in `.env`

5. Run database migrations:
   ```bash
   uv run alembic upgrade head
   ```

6. Start the application:
   ```bash
   uv run python -m app.main
   ```

## API Endpoints

### Products
- `GET /api/products` - List products with filters
- `GET /api/products/{product_id}` - Get specific product
- `GET /api/products/{product_id}/history` - Get price history
- `GET /api/products/{product_id}/full` - Get product with history

### Scraping
- `GET /api/scrapers/status` - Get scraping status
- `POST /api/scrapers/{supermarket}/refresh-products` - Trigger sitemap refresh
- `POST /api/scrapers/{supermarket}/scan-batch` - Trigger batch scan
- `POST /api/scrapers/{supermarket}/products/{product_id}` - Scan single product

### Health & Stats
- `GET /api/health` - Health check
- `GET /api/stats/products/count` - Product count
- `GET /api/stats/price-changes` - Price change statistics

## Configuration

Key environment variables:

- `DATABASE_URL`: PostgreSQL connection string
- `BATCH_SIZE_FRACTION`: Fraction of products per batch (default: 1/48)
- `BATCH_DURATION_MINUTES`: Time to spread requests (default: 10)
- `CONCURRENT_REQUESTS`: Number of concurrent requests (default: 15)

## Development

### Running Tests
```bash
uv run pytest
```

### Code Formatting
```bash
uv run black .
uv run isort .
```

### Database Migrations
```bash
# Create new migration
uv run alembic revision --autogenerate -m "Description"

# Apply migrations
uv run alembic upgrade head
```

## License

MIT License
