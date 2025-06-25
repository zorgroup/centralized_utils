
import time
import json
import logging
from typing import Optional, Union, List, Dict

class LogController:
    def __init__(self, scraper_name: str):
        """namespace: Shared CloudWatch namespace for metrics."""
        self.namespace = 'ws_main_v1'
        self.logger = self._setup_logging()
        self.scraper_name = scraper_name
        self.emf_dimensions = [["Outcome", "Retailer", "ProxyId"]]

    def _setup_logging(self) -> logging.Logger:
        """Configure logging for the main script"""
        logging.addLevelName(logging.INFO, "Patrick")
        logger = logging.getLogger(__name__)
        if not logger.handlers:
            logger.setLevel(logging.INFO)
            ch = logging.StreamHandler()
            ch.setFormatter(logging.Formatter('%(message)s'))
            logger.addHandler(ch)
        return logger

    def log_request(
        self,
        total_product_count: int,
        sanitized_product_count: int,
        response_time_ms: Optional[float],
        status: Optional[int],
        error_msg: Optional[str],
        redis_urls: List[str],
        proxy_id: str,
    ) -> str:
        """
        Emits EMF payloads for CloudWatch.
        This function will log one of the following request outcomes:
            1. success
            2. proxy_issue
            3. scraper_issue
        
        If outcome == "success", emits an extra ProductCount metric.

        Scraper Variations in regards to how they handle Redis URLs, and then how these effect log behaviour:
            1. Product URLs based: Redis contains Product URLs, redis goes down by 1, scraper makes 1 request, extracts 1 product, emits 1 log outcome "success/proxy_issue/scraper_issue". Example: ws_oreilly_m9, ws_amazon_m1
            2. Pagination URLs based: Redis contains Pagination URLs, redis goes down by 1, scraper makes 1 request, extracts multiple (e.g. 15) products, emits 1 log outcome "success/proxy_issue/scraper_issue". Example: ws_rockauto_m2, ws_finditparts_m4, ws_walmart_m4
            3. Bulk APIs based: Redis contains Product URLs/IDs, redis goes down by bulk (e.g. 50), scraper makes 1 request to a bulk API, extracts 50 products, emits 50 log outcomes "success/proxy_issue/scraper_issue". e.g. ws_fleetpride_m1, ws_autozone_m2.
        """

        # --- Value Assertions ---
        if type(total_product_count) != int:
            raise ValueError(f'total_product_count must be int, received: {type(total_product_count)}')
        # --- Value Assertions ---
        if type(sanitized_product_count) != int:
            raise ValueError(f'sanitized_product_count must be int, received: {type(sanitized_product_count)}')
        if not proxy_id or not redis_urls:
            raise ValueError("Both 'proxy_id' and 'urls' must be provided.")
        if type(redis_urls) != list:
            raise ValueError(f'redis_urls must be list[str], received: {type(redis_urls)}')

        # --- Normalize response_time ---
        if response_time_ms is None:
            response_time_ms = 0.0

        # --- Calculate sanitization rate ---
        sanitization_rate = (sanitized_product_count / total_product_count * 100) if total_product_count else 0

        # --- Determine outcome ---
        if status == 200 and sanitization_rate >= 50.0 and sanitized_product_count > 0:
            outcome = 'success'
        elif status in (403, 429):
            outcome = 'proxy_issue'
        else:
            outcome = 'scraper_issue'

        # --- Calculate Number of Logs that we need to emit ---
        # The number of logs we emit, must be equal to the number of urls that were popped from redis, even if all those urls were scraped using a single API request.
        emit_multiple_logs = True if len(redis_urls) > 1 else False
        if emit_multiple_logs:
            # Log outcome for each redis_url. (Scraper Variation 3)
            number_of_logs_to_emit = len(redis_urls)
        else:
            # Log single outcome (Scraper Variation 1 and 2)
            number_of_logs_to_emit = 1

        # --- Build metric definitions ---
        cw_metrics = [{
            "Namespace": self.namespace,
            "Dimensions": self.emf_dimensions,
            "Metrics": [
                {"Name": "ResponseTime", "Unit": "Milliseconds"}
            ]
        }]

        # Only for success, include additional ProductCount metric
        if outcome == 'success':
            cw_metrics[0]["Metrics"].append({"Name": "ProductCount", "Unit": "Count"})

        # --- Build payload ---
        payload: Dict[str, Union[str, int, float, dict, list]] = {
            "_aws": {
                #"Timestamp": int(time.time() * 1000),
                "CloudWatchMetrics": cw_metrics
            },
            "Outcome": outcome,
            "Retailer": self.scraper_name,
            "ProxyId": proxy_id,
            "ResponseTime": response_time_ms,
        }

        # Add standard metadata
        payload.update({
            #"Url": redis_urls,
            "StatusCode": status,
            "SanitizationRate": sanitization_rate,
        })
        if error_msg:
            payload["Error"] = error_msg

        # Add ProductCount field in payload only on success.
        #   If we are emiting a single success logs (Scraper Variation 1 and 2), we want to count all the products in that single log.
        #   If we are emiting multiple success logs (Scraper Variation 3), we want to count a single product per each success log.
        if outcome == 'success':
            payload["ProductCount"] = 1 if emit_multiple_logs else sanitized_product_count

        # --- Emit the logs ---
        for i in range(number_of_logs_to_emit):
            # Update timestamp and url for each emitted log.
            payload['_aws']['Timestamp'] = int(time.time() * 1000)
            payload['Url'] = redis_urls[i]
            # Emit the log
            self.logger.info("")  # blank line
            self.logger.info(json.dumps(payload))
            self.logger.info("")  # blank line

        return outcome



    def log_processing_error(self, message: str, proxy_id: Optional[str] = None):
        '''Use this function to log these 'processing_error' outcome, these are generic errors anywhere in the code'''
        # Create EMF payload.
        cw_metrics = [{
                "Namespace": self.namespace,
                "Dimensions": self.emf_dimensions,
                "Metrics": [{"Name": "RequestCount", "Unit": "Count"}]
            }]
        payload = {
            "_aws": {"Timestamp": int(time.time() * 1000), "CloudWatchMetrics": cw_metrics},
            "Outcome": 'processing_error',
            "Retailer": self.scraper_name,
            "ProxyId": proxy_id if proxy_id else 'N/A',
            "RequestCount": 1,
        }

        # Add additional metadata.
        payload.update({
            'Error': message,
        })
        
        # Print the EMF log.
        self.logger.info('\n')
        self.logger.info(json.dumps(payload))
        self.logger.info('\n')


    def log_s3_upload(self, product_count, file_name, products_type, proxy_id: Optional[str] = None):

        # Create EMF payload.
        cw_metrics = [{
                "Namespace": self.namespace,
                "Dimensions": self.emf_dimensions,
                "Metrics": [{"Name": "ProductCount", "Unit": "Count"}]
            }]
        payload = {
            "_aws": {"Timestamp": int(time.time() * 1000), "CloudWatchMetrics": cw_metrics},
            "Outcome": 's3_upload',
            "Retailer": self.scraper_name,
            "ProxyId": proxy_id if proxy_id else 'N/A',
            "ProductCount": product_count,
        }

        # Add additional metadata.
        payload.update({
            "ProductsType": products_type   # Whether these products are seen or unseen.
        })
        
        # Print the EMF log.
        self.logger.info('\n')
        self.logger.info(json.dumps(payload))
        self.logger.info('\n')

        # Print info message for visual purpose.
        info_message = (
            "\n"  # Padding above.
            "*******************************************************************************************\n"
            f"{product_count} {products_type} products inserted into s3. Filename: {file_name}\n"
            "*******************************************************************************************"
            "\n"  # Padding below.
        )
        self.logger.info(info_message)


    def log_products(self, products: list[dict], proxy_id: str):
        """This function takes a list of sanitized products and loggs the essential details"""

        if type(products) != list:
            raise ValueError(f"'products' must be of type 'list[dict]'. Got '{type(products)}' instead")
        
        # Create EMF payload.
        cw_metrics = [{
                "Namespace": self.namespace,
                "Dimensions": self.emf_dimensions,
                "Metrics": [{"Name": "ProductCount", "Unit": "Count"}]
            }]
        payload = {
            "_aws": {"Timestamp": int(time.time() * 1000), "CloudWatchMetrics": cw_metrics},
            "Outcome": 'products',
            "Retailer": self.scraper_name,
            "ProxyId": proxy_id,
            "ProductCount": len(products),
        }

        # Print the EMF log.
        self.logger.info('\n')
        self.logger.info(json.dumps(payload))
        self.logger.info('\n')
        
        # Print product data for visual purpose.
        for product in products:
            product_info = {
                'product_url': product['product_url'],
                'price': product['price'] if 'price' in product else None,
                'in_stock': product['in_stock'],
                'currency': product['currency']
            }
            self.logger.info(f'product: {json.dumps(product_info)}')

    
    def log_info(self, message: str):
        '''Prints any general purpose (informational) message'''
        payload = f'Info: {message}'
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










