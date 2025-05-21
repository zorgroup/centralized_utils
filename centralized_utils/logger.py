import time
import json
import logging
from typing import Optional

class LogController:
    def __init__(self, scraper_name):
        """namespace: Shared CloudWatch namespace for metrics."""
        self.namespace = 'ws_main'
        self.logger = self._setup_logging()
        self.scraper_name = scraper_name


    def _setup_logging(self):
        """Configure logging for the main script"""
        # Map the INFO logging level to "Patrick"
        logging.addLevelName(logging.INFO, "Patrick")
        logger = logging.getLogger(__name__)
        if not logger.handlers:
            logger.setLevel(logging.INFO)
            # Console handler
            ch = logging.StreamHandler()
            ch.setFormatter(logging.Formatter('%(message)s'))
            logger.addHandler(ch)
            return logger


    def log_request(
        self,
        outcome: str,
        url: str,
        proxy: str,
        response_time_ms: Optional[float] | None = None,
        status_code: Optional[int] | None = None,
        message: Optional[str] | None = None,
        stats: Optional[dict] | None = None
    ):
        """
        Emits an EMF payload for CloudWatch.
        Use this function to log one of these outcomes: 
            1. success
            2. proxy_issue 
            3. scraper_issue 

        Args:
          outcome: 'success', 'proxy_issue', or 'scraper_issue'
          response_time_ms: elapsed time in ms
          retailer: used as EMF dimension
          proxy: proxy URL used
          url: request URL
          stats: dict with keys:
            - num_of_req
            - num_of_successful_req
            - total_products_found
            - num_of_sanitized_products
            - total_saved_to_s3
        """
        
        # Value Assertions
        if outcome not in ('success', 'proxy_issue', 'scraper_issue'):
            raise ValueError(f'Unsupported outcome: {outcome}')
        if not proxy or not url:
            raise ValueError("Both 'proxy' and 'url' must be provided.")
        if outcome == 'proxy_issue':
            if response_time_ms == None:
                raise ValueError("Parameter 'response_time_ms' is mandatory if 'outcome' == 'proxy_issue'")

        # Create EMF payload.
        if outcome == 'proxy_issue':
            cw_metrics = [{
                "Namespace": self.namespace,
                "Dimensions": [["Outcome", "Retailer"]],
                "Metrics": [{"Name": "ResponseTime", "Unit": "Milliseconds"}]
            }]
            payload = {
                "_aws": {"Timestamp": int(time.time() * 1000), "CloudWatchMetrics": cw_metrics},
                "Outcome": outcome,
                "Retailer": self.scraper_name,
                "ResponseTime": response_time_ms,
            }
        else:
            cw_metrics = [{
                "Namespace": self.namespace,
                "Dimensions": [["Outcome", "Retailer"]],
                "Metrics": [{"Name": "RequestCount", "Unit": "Count"}]
            }]
            payload = {
                "_aws": {"Timestamp": int(time.time() * 1000), "CloudWatchMetrics": cw_metrics},
                "Outcome": outcome,
                "Retailer": self.scraper_name,
                "RequestCount": 1,
            }

        # Add additional fields
        payload.update({
            "Url": url,
            "Proxy": proxy,
            "StatusCode": status_code,
            "Message": message,
        })

        # Only insert stats fields in log event if stats != None
        if stats:
            success_rate = (
                round(100 * stats['num_of_successful_req'] / stats['num_of_req'], 2)
                if stats.get('num_of_req') else 0
            )
            sanitization_rate = (
                round(100 * stats['num_of_sanitized_products'] / stats['total_products_found'], 2)
                if stats.get('total_products_found') else 0
            )
            payload.update({
                "TotalRequests": stats.get('num_of_req'),
                "SuccessfulRequests": stats.get('num_of_successful_req'),
                "SuccessRate%": success_rate,
                "TotalProductsFound": stats.get('total_products_found'),
                "TotalProductsSanitized": stats.get('num_of_sanitized_products'),
                "SanitizationRate$": sanitization_rate,
                "TotalSavedToS3": stats.get('total_saved_to_s3')
            })

        self.logger.info('\n')
        self.logger.info(json.dumps(payload))
        self.logger.info('\n')


    def log_sanitization_error(self, product: dict, error: str):
        '''
        Use this function to log outcome 'sanitization_error'
        '''
        # Create EMF payload.
        cw_metrics = [{
                "Namespace": self.namespace,
                "Dimensions": [["Outcome", "Retailer"]],
                "Metrics": [{"Name": "RequestCount", "Unit": "Count"}]
            }]
        payload = {
            "_aws": {"Timestamp": int(time.time() * 1000), "CloudWatchMetrics": cw_metrics},
            "Outcome": 'sanitization_error',
            "Retailer": self.scraper_name,
            "RequestCount": 1,
        }

        # Add additional fields.
        payload.update({
            "product": product,
            "message": f'failed to sanitize product - {error}'
        })
        
        self.logger.info('\n')
        self.logger.info(json.dumps(payload))
        self.logger.info('\n')


    def log_processing_error(self, message: str):
        '''Use this function to log these 'processing_error' outcome, these are generic errors anywhere in the code'''
        # Create EMF payload.
        cw_metrics = [{
                "Namespace": self.namespace,
                "Dimensions": [["Outcome", "Retailer"]],
                "Metrics": [{"Name": "RequestCount", "Unit": "Count"}]
            }]
        payload = {
            "_aws": {"Timestamp": int(time.time() * 1000), "CloudWatchMetrics": cw_metrics},
            "Outcome": 'processing_error',
            "Retailer": self.scraper_name,
            "RequestCount": 1,
        }

        # Add additional fields.
        payload.update({
            'message': message,
        })
        
        self.logger.info('\n')
        self.logger.info(json.dumps(payload))
        self.logger.info('\n')


    def log_info(self, message: str):
        '''Prints any general purpose (informational) message'''
        payload = f'Info: {message}'
        self.logger.info(payload)


    def log_s3_update(self, product_count, file_name, type):
        """This function takes the stats dict and loggs the relavent details"""

        # Build a single log message with padding and block separators.
        payload = (
            "\n"  # Padding above.
            "*******************************************************************************************\n"
            f"{product_count} {type} products inserted into s3. Filename: {file_name}\n"
            "*******************************************************************************************"
            "\n"  # Padding below.
        )
        self.logger.info(payload)


    def log_stats(self, stats: dict):
        """Simplified json print with some padding for visual separation."""
        
        payload = (
            "\n"  # Padding above.
            "*******************************************************************************************\n"
            f"Stats: {json.dumps(stats)}\n"
            "*******************************************************************************************"
            "\n"  # Padding below.
        )
        self.logger.info(payload)


    def log_product_info(self, sanitized_product):
        """This function takes the sanitized product dict and loggs the essential details"""

        payload = {
            'product_url': sanitized_product['product_url'],
            'price': sanitized_product['price'],
            'in_stock': sanitized_product['in_stock'],
            'currency': sanitized_product['currency']
        }
        self.logger.info(f'product: {json.dumps(payload)}')
