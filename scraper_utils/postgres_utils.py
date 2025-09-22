import os
import asyncpg
from .context import GlobalScraperContext


async def initialize_postgres_client(context: GlobalScraperContext):
    try:
        if context.running_environment == 'prod':
            context.postgres_host_prod = os.environ['POSTGRES_HOST_PROD']
            context.postgres_port_prod = os.environ.get('POSTGRES_PORT_PORT', 5432)
            context.postgres_user_prod = os.environ['POSTGRES_USER_PROD']
            context.postgres_pass_prod = os.environ['POSTGRES_PASS_PROD']
            context.postgres_dbname_prod = os.environ['POSTGRES_DBNAME_PROD']
            pool = await asyncpg.create_pool(
                host=context.postgres_host_prod,
                port=context.postgres_port_prod,
                database=context.postgres_dbname_prod,
                user=context.postgres_user_prod,
                password=context.postgres_pass_prod,
                min_size=1,
                max_size=30
            )
        elif context.running_environment == 'dev':
            context.postgres_host_dev = os.environ['POSTGRES_HOST_DEV']
            context.postgres_port_dev = os.environ.get('POSTGRES_PORT_DEV', 5432)
            context.postgres_user_dev = os.environ['POSTGRES_USER_DEV']
            context.postgres_pass_dev = os.environ['POSTGRES_PASS_DEV']
            context.postgres_dbname_dev = os.environ['POSTGRES_DBNAME_DEV']
            pool = await asyncpg.create_pool(
                host=context.postgres_host_dev,
                port=context.postgres_port_dev,
                database=context.postgres_dbname_dev,
                user=context.postgres_user_dev,
                password=context.postgres_pass_dev,
                min_size=1,
                max_size=30
            )
        context.postgres_client = pool
    except Exception as e:
        context.logger.log_processing_error(f"Could not connect to postgres - {str(e)}", proxy_id=None)
        raise e
    


async def close_postgres_client(context: GlobalScraperContext):
    if context.postgres_client:
        pool = context.postgres_client
        await pool.close()



async def load_scraper_configuration(context: GlobalScraperContext):
    try:
        # Read scraper configuration from postgres and update container_state in a single transaction.
        pool = context.postgres_client
        async with pool.acquire() as conn:
            async with conn.transaction():
                query = "SELECT * FROM scrapers.scrapers_configuration WHERE scraper_name = $1;"
                row = await conn.fetchrow(query, context.scraper_name)
                psql_scraper_config = dict(row)

                # Set container_state to 1.
                query = "UPDATE scrapers.scrapers_configuration SET container_state = $1 WHERE scraper_name = $2;"
                await conn.execute(query, 1, context.scraper_name)
         
        context.scraper_state_key = psql_scraper_config['scraper_state_key']
        context.concurrency = psql_scraper_config['concurrency']
        context.request_delay = psql_scraper_config['request_delay']
        context.redis_batch_size = psql_scraper_config['urls_per_batch']
        context.s3_bulk_size = psql_scraper_config['s3_bulk_size']
        context.s3_bucket_name = psql_scraper_config['bucket_name']
        context.redis_source_key_temp = psql_scraper_config['source_key_temp']
        context.redis_seen_products_key = psql_scraper_config['seen_products_key']
        context.proxy_ids = psql_scraper_config['proxy_ids'] 
        context.scraper_type = psql_scraper_config['scraper_type']

        if context.running_environment == 'prod':
            context.redis_host_prod = psql_scraper_config['redis_host']                       
            context.redis_port_prod = psql_scraper_config['redis_port']                        
        elif context.running_environment == 'dev':                                            
            context.redis_host_dev = psql_scraper_config['redis_host']    
            context.redis_port_dev = psql_scraper_config['redis_port'] 
        
    except Exception as e:
        context.logger.log_processing_error(f"Error in loading scraper configuration - {str(e)}", proxy_id=None)



async def check_if_restart_required(context: GlobalScraperContext) -> bool:
    '''
    Check if the container/scraper needs to restart (to load new config).
    '''
    try:
        # Read scraper configuration from postgres.
        pool = context.postgres_client
        async with pool.acquire() as conn:
            query = "SELECT container_state FROM scrapers.scrapers_configuration WHERE scraper_name = $1;"
            row = await conn.fetchrow(query, context.scraper_name)
            container_state = row[0]
            
            # Set exit code to 1. This is important, so scraper would restart with new configuration.
            if container_state == 0:
                context.exit_code = 1
                return True
            else:
                return False
            
    except Exception as e:
        context.logger.log_processing_error(message=f"Error while checking container state - {str(e)}", proxy_id=None)
