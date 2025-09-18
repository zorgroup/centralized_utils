import os
import redis.asyncio as aioredis
from datetime import datetime, timezone
from .context import GlobalScraperContext



async def initialize_redis_client(context: GlobalScraperContext):
    '''
    Initialize and return redis client based on RUNNING_ENVIRONMENT.
    '''
    try:
        if context.running_environment == 'prod':
            redis_client = aioredis.Redis(
                host=context.redis_host_prod, port=context.redis_port_prod, ssl=True,
                socket_connect_timeout=30
            )
            await redis_client.ping()
        elif context.running_environment == 'dev':
            context.redis_pass_dev = os.environ['REDIS_PASS_DEV']
            context.redis_dbnumber_dev = os.environ.get('REDIS_DBNUMBER_DEV', 0)
            redis_client = aioredis.Redis(
                    host=context.redis_host_dev,
                    port=context.redis_port_dev,
                    password=context.redis_pass_dev,
                    db=context.redis_dbnumber_dev,
                    socket_connect_timeout=30
                )
            await redis_client.ping()
        
        context.redis_client = redis_client
    
    except Exception as e:
        context.logger.log_processing_error(message="Failed to connect to redis. If you testing 'prod' on a 'dev' server, make sure that you are connected to OpenVPN.", proxy_id=None)
        raise e
    


async def close_redis_client(context: GlobalScraperContext):
    if context.redis_client:
        await context.redis_client.aclose()



async def pop_urls_from_redis_temp(context: GlobalScraperContext) -> list[str]:
    urls = await context.redis_client.spop(context.redis_source_key_temp, context.redis_batch_size)
    return [url.decode('utf-8') for url in urls] if urls else []



async def insert_urls_into_redis(context: GlobalScraperContext, destination_key: str, urls: list[str]):
    try:
        await context.redis_client.sadd(destination_key, *urls)
        context.logger.log_info(message=f"Inserted {len(urls)} urls into redis set '{destination_key}'")
    except Exception as e:
        context.logger.log_processing_error(message=f'Failed to insert urls to redis: {e}', proxy_id=context.proxy_ids[0])



async def load_scraper_state(context: GlobalScraperContext):
    raw = await context.redis_client.get(context.scraper_state_key)
    if raw:
        # aioredis returns bytes by default
        scraper_state = raw.decode() if isinstance(raw, bytes) else raw
    else:
        scraper_state = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    context.scraper_state = scraper_state