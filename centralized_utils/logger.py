import time
import json
import logging
from typing import Optional

class LogController:
    def __init__(self, namespace: str):
        """namespace: Shared CloudWatch namespace for metrics."""
        self.namespace = namespace
        self.logger = self._setup_logging()

    def _setup_logging(self):
        logging.addLevelName(logging.INFO, "Patrick")
        logger = logging.getLogger(__name__)
        if not logger.handlers:
            logger.setLevel(logging.INFO)
            ch = logging.StreamHandler()
            ch.setFormatter(logging.Formatter(' %(message)s'))
            logger.addHandler(ch)
        return logger

    def log_request(
        self,
        outcome: str,
        response_time_ms: float,
        *,
        retailer: str,
        status_code: Optional[int] = None,
        proxy: str,
        url: str,
        message: Optional[str] = None,
        stats: dict
    ):
        """
        Emit an EMF payload for CloudWatch.

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
        if outcome not in ('success', 'proxy_issue', 'scraper_issue'):
            raise ValueError(f'Unsupported outcome: {outcome}')
        if not proxy or not url:
            raise ValueError("Both 'proxy' and 'url' must be provided.")

        success_rate = (
            round(100 * stats['num_of_successful_req'] / stats['num_of_req'], 2)
            if stats.get('num_of_req') else 0
        )
        sanitization_rate = (
            round(100 * stats['num_of_sanitized_products'] / stats['total_products_found'], 2)
            if stats.get('total_products_found') else 0
        )

        # prepare EMF
        if outcome == 'proxy_issue':
            cw_metrics = [{
                "Namespace": self.namespace,
                "Dimensions": [["Outcome", "Retailer"]],
                "Metrics": [{"Name": "ResponseTime", "Unit": "Milliseconds"}]
            }]
            payload = {
                "_aws": {"Timestamp": int(time.time() * 1000), "CloudWatchMetrics": cw_metrics},
                "Outcome": outcome,
                "Retailer": retailer,
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
                "Retailer": retailer,
                "RequestCount": 1,
            }

        # add context fields
        payload.update({
            "Proxy": proxy,
            "Url": url,
            "status_code": status_code,
            "message": message,
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
