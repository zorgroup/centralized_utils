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
        sanitization_rate: float|int,
        response_time_ms: float|None,
        status: int|None,
        error_msg: str|None,
        urls: list[str],
        proxy_id: str,
    ):
        """
        Emits an EMF payload for CloudWatch.
        This function will log one of the following request outcomes: 
            1. success
            2. proxy_issue 
            3. scraper_issue 
        """
        
        # Value Assertions
        if type(sanitization_rate) not in [float, int]:
            raise ValueError(f'sanitization_rate must be float or int, recieved: {sanitization_rate}')
        if not proxy_id or not urls:
            raise ValueError("Both 'proxy_id' and 'urls' must be provided.")

        # Set response_time to 0.0 milisecond if not available.
        if response_time_ms == None:
            response_time_ms = 0.0

        # Analyze the request to determine outcome.
        if status==200 and sanitization_rate >= 50.0:
            outcome = 'success'
        elif status in [403, 429]:
            outcome = 'proxy_issue'
        else:
            outcome = 'scraper_issue'

        # Create EMF payload.
        cw_metrics = [{
                "Namespace": self.namespace,
                "Dimensions": [["Outcome", "Retailer", "ProxySubscriptionId"]],
                "Metrics": [{"Name": "ResponseTime", "Unit": "Milliseconds"}]
            }]
        payload = {
            "_aws": {"Timestamp": int(time.time() * 1000), "CloudWatchMetrics": cw_metrics},
            "Outcome": outcome,
            "Retailer": self.scraper_name,
            "ProxySubscriptionId": proxy_id,
            "ResponseTime": response_time_ms,
        }

        # Add additional fields to payload.
        payload.update({
            "Urls": urls,
            "StatusCode": status,
            "SanitizationRate": sanitization_rate
        })
        if error_msg:
            payload.update({
                'Error': error_msg
            })

        # Generate log.
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


    def log_product_info(self, product: dict):
        """This function takes the sanitized product dict and loggs the essential details"""

        if type(product) != dict:
            raise ValueError(f"'product' must be of type 'dict'. Got '{type(product)}' instead")
        
        payload = {
            'product_url': product['product_url'],
            'price': product['price'],
            'in_stock': product['in_stock'],
            'currency': product['currency']
        }
        self.logger.info(f'product: {json.dumps(payload)}')


    def log_products_info(self, products: list[dict]):
        """This function takes a list of sanitized products and loggs the essential details"""

        if type(products) != list:
            raise ValueError(f"'products' must be of type 'list[dict]'. Got '{type(products)}' instead")
        
        for product in products:
            self.log_product_info(product)









