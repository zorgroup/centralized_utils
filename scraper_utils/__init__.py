"""
Centralized utilities for web scrapers.
Provides reusable components for Logging, Redis, Postgres, S3, data processing, and state management via a Context Object.
"""

# Context Object
from .context import GlobalScraperContext
from .postgres_utils import initialize_postgres_client, load_scraper_configuration, check_if_restart_required, close_postgres_client
from .redis_utils import initialize_redis_client, pop_source_urls_from_redis_temp, pop_sources_from_redis, insert_failed_source_urls_into_redis_temp, insert_failed_sources_into_redis, load_scraper_state, close_redis_client
from .s3_utils import get_current_quarter_number, initialize_s3_client, upload_to_s3, close_s3_client
from .proxy_utils import load_proxies
from .data_processing_utils import sanitize_products
from .logger_utils import initialize_logger