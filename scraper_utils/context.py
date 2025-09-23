
import os
from datetime import datetime, timezone

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import boto3
    import asyncpg
    from typing import List
    import redis.asyncio as aioredis
    from .proxy_utils import Proxy
    from .logger_utils import AWSLogger



class GlobalScraperContext():
    '''
    Holds global references for the scraper, including database connections, credentials, and other configurations.
    Any function that receives this context will get an easy access to all variables declared/initialized within this object.
    '''

    def __init__(self):
        # Configuraton
        self.scraper_name:                   str                     = None
        self.retailer_name:                  str                     = None
        self.scraper_state:                  str                     = None
        self.scraper_method_summary:         str                     = None
        self.running_environment:            str                     = os.getenv('RUNNING_ENVIRONMENT').lower()
        self.proxy_ids:                      List[str]               = None
        self.proxies_list:                   List[Proxy]             = None
        self.concurrency:                    int                     = None
        self.scraper_state_key:              str                     = None
        self.request_delay:                  float                   = None
        self.redis_batch_size:               int                     = None
        self.redis_source_key_temp:          str                     = None
        self.redis_seen_products_key:        str                     = None
        self.s3_bulk_size:                   int                     = None
        self.s3_bucket_name:                 str                     = None
        self.scraper_type:                   str                     = None

        # Services
        self.logger:                         AWSLogger               = None
        self.redis_client:                   aioredis.Redis          = None
        self.postgres_client:                asyncpg.Connection      = None
        self.s3_client:                      boto3.client            = None

        # Misc
        self.scraper_start_time:             datetime                = datetime.now(timezone.utc)
        self.exit_code:                      int                     = 0
        
        # Credentials
        self.postgres_host_dev:              str                     = None
        self.postgres_port_dev:              int                     = None
        self.postgres_user_dev:              str                     = None
        self.postgres_pass_dev:              str                     = None
        self.postgres_dbname_dev:            str                     = None
        self.postgres_host_prod:             str                     = None
        self.postgres_port_prod:             int                     = None
        self.postgres_user_prod:             str                     = None
        self.postgres_pass_prod:             str                     = None
        self.postgres_dbname_prod:           str                     = None
        self.redis_host_prod:                str                     = None
        self.redis_port_prod:                int                     = None
        self.redis_host_dev:                 str                     = None
        self.redis_port_dev:                 int                     = None
        self.redis_pass_dev:                 str                     = None
        self.redis_dbnumber_dev:             int                     = None
        self.aws_access_key_id_dev:          str                     = None
        self.aws_secret_access_key_dev:      str                     = None
        self.aws_access_key_id_prod:         str                     = None
        self.aws_secret_access_key_prod:     str                     = None
        self.api_keys:                       dict                    = None

        print('Using Scraper Utils v1.0')


    def confirm_all_mandatory_fields_are_initialized(self):
        '''Raises an exception if any of the mandatory global context variables are not initialized.'''
        if not self.scraper_name:
            raise Exception(f"Missing mandatory context variable: 'scraper_name'")
        if not self.retailer_name:
            raise Exception(f"Missing mandatory context variable: 'retailer_name'")
        if not self.scraper_state:
            raise Exception(f"Missing mandatory context variable: 'scraper_state'")
        if not self.scraper_method_summary:
            raise Exception(f"Missing mandatory context variable: 'scraper_method_summary'")
        if not self.running_environment:
            raise Exception(f"Missing mandatory context variable: 'running_environment'")
        if not self.logger:
            raise Exception(f"Missing mandatory context variable: 'logger'")
        if not self.redis_client:
            raise Exception(f"Missing mandatory context variable: 'redis_client'")
        if not self.postgres_client:
            raise Exception(f"Missing mandatory context variable: 'postgres_client'")
        if not self.s3_client:
            raise Exception(f"Missing mandatory context variable: 's3_client'")
        if not self.concurrency:
            raise Exception(f"Missing mandatory context variable: 'concurrency'")
        if not self.redis_batch_size:
            raise Exception(f"Missing mandatory context variable: 'redis_batch_size'")
        if not self.s3_bulk_size:
            raise Exception(f"Missing mandatory context variable: 's3_bulk_size'")
        if not self.s3_bucket_name:
            raise Exception(f"Missing mandatory context variable: 's3_bucket_name'")
        if not self.scraper_type:
            raise Exception(f"Missing mandatory context variable: 'scraper_type'")
        if not self.redis_source_key_temp:
            raise Exception(f"Missing mandatory context variable: 'redis_source_key_temp'")