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


# Set variant
async def pop_source_urls_from_redis_temp(context: GlobalScraperContext) -> list[str]:
    urls = await context.redis_client.spop(context.redis_source_key_temp, context.redis_batch_size)
    context.logger.log_info(f'Popped {len(urls)} from redis.')
    return [url.decode('utf-8') for url in urls] if urls else []


# Set variant
async def insert_failed_source_urls_into_redis_temp(context: GlobalScraperContext, urls: list[str]):
    try:
        await context.redis_client.sadd(context.redis_source_key_temp, *urls)
        context.logger.log_info(message=f"Inserted {len(urls)} failed urls into redis set '{context.redis_source_key_temp}'")
    except Exception as e:
        context.logger.log_processing_error(message=f'Failed to insert urls to redis: {e}', proxy_id=context.proxy_ids[0])



# Hash variant
async def pop_sources_from_redis(context: GlobalScraperContext) -> list[dict]:
    """
    Atomically pop urls from Temp Hash for scraping.
    Returns a list of dict containing Url and Retry Count.
    """

    # Use Lua script, to combine HRANDFIELD and HDEL into a signle atomic operation, simulating a Pop.
    LUA_SCRIPT_FOR_POPPING = """
    local key = KEYS[1]
    local count = tonumber(ARGV[1])
    local result = redis.call('HRANDFIELD', key, count, 'WITHVALUES')
    if #result == 0 then
        return {}
    end
    for i = 1, #result, 2 do
        redis.call('HDEL', key, result[i])
    end
    return result
    """

    # Create a reusable sha for popping lua script.
    if not context.redis_sha_for_popping:
        context.redis_sha_for_popping = await context.redis_client.script_load(LUA_SCRIPT_FOR_POPPING)

    # Pop urls from redis hash using script sha.
    flat_raw_list = await context.redis_client.evalsha(context.redis_sha_for_popping, 1, context.redis_temp_key, context.redis_batch_size)

    if not flat_raw_list:  # No urls in redis.
        return []

    source_urls_with_retry_count = [
        {"url": flat_raw_list[i].decode(), "retries": int(flat_raw_list[i + 1].decode())}
        for i in range(0, len(flat_raw_list), 2)
    ]

    context.logger.log_info(
        f'Popped {len(source_urls_with_retry_count)} urls from redis hash "{context.redis_source_key_temp}".'
    )

    return source_urls_with_retry_count


# Hash variant
async def insert_failed_sources_into_redis(context: GlobalScraperContext, sources: list[dict]):
    """
    Reinsert failed URLs into the Temp Hash (with incremented retry counts),
    or increment them in Failed Hash if max retries exceeded.
    """
    try:
        async with context.redis_client.pipeline(transaction=False) as pipe:
            for source in sources:
                url = source["url"]
                old_retry_count = int(source["retries"])
                new_retry_count = old_retry_count + 1

                if new_retry_count <= context.max_retries_same_cycle:
                    # Still eligible for retry — reinsert with incremented count
                    pipe.hset(context.redis_temp_key, url, new_retry_count)
                else:
                    # Retries exhausted — increment fail counter
                    pipe.hincrby(context.redis_failed_key, url, 1)

            # Execute all queued commands in one batch
            await pipe.execute()

        context.logger.log_info(
            message=f"Inserted {len(sources)} failed urls to Redis."
        )
    
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