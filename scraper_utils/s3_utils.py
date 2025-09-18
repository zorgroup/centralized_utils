import os
import json
import uuid
import boto3
import asyncio
import hashlib
from datetime import datetime, timezone
from .context import GlobalScraperContext



def get_current_quarter_number(now):
    """
    Determine the current quarter number based on the hour of the day (UTC).
    Quarter 1: 00:00 - 05:59 UTC
    Quarter 2: 06:00 - 11:59 UTC
    Quarter 3: 12:00 - 17:59 UTC
    Quarter 4: 18:00 - 23:59 UTC
    """
    hour = now.hour
    if 0 <= hour < 6:
        return '1'
    elif 6 <= hour < 12:
        return '2'
    elif 12 <= hour < 18:
        return '3'
    else:
        return '4'
    


async def split_unseen_seen_products(context: GlobalScraperContext, product_buffer: list):
    """
    Evaluate whether products have metadata by attempting to add them to Redis set 'retailer_seen_urls'.
    If URL already existed in Redis set, append product to seen_products list.
    If URL is newly inserted, append product to unseen_products list.
    """
    
    # Add tasks to redis pipeline.
    pipeline = context.redis_client.pipeline()
    for product in product_buffer:
        # Extract url from each product, and create task for adding the url to redis.
        url = product['product_url']
        pipeline.sadd(context.redis_seen_products_key, url)

    # Execute the pipeline in one batch.
    results = await pipeline.execute()  

    # Categorize Products based on the Redis response.
    unseen_products = []
    seen_products = []

    for product, res in zip(product_buffer, results):
        if res == 1:
            unseen_products.append(product)  # Successfully inserted. This product should be saved to s3 retailer_daily_unseen folder.
        else:
            seen_products.append(product)  # Already existed. This product should be saved to s3 daily_pricing folder.
    
    return unseen_products, seen_products



def initialize_s3_client(context: GlobalScraperContext):
    try:
        if context.running_environment == 'prod':    # Iniitialize s3 for prod.
            context.aws_access_key_id_prod = os.environ.get('AWS_ACCESS_KEY_ID_PROD', None)
            context.aws_secret_access_key_prod = os.environ.get('AWS_SECRET_ACCESS_KEY_PROD', None)
            s3_client = boto3.client(
                's3',
                aws_access_key_id=context.aws_access_key_id_prod,
                aws_secret_access_key=context.aws_secret_access_key_prod
            )

        elif context.running_environment == 'dev':   # Iniitialize s3 for dev.
            context.aws_access_key_id_dev = os.environ['AWS_ACCESS_KEY_ID_DEV']
            context.aws_secret_access_key_dev = os.environ['AWS_SECRET_ACCESS_KEY_DEV']
            s3_client = boto3.client(
                's3',
                aws_access_key_id=context.aws_access_key_id_dev,
                aws_secret_access_key=context.aws_secret_access_key_dev
            )
        
        context.s3_client = s3_client
    
    except Exception as e:
        context.logger.log_processing_error(message="Error connecting to s3.", proxy_id=None)
        raise e
    


def close_s3_client(context: GlobalScraperContext):
    if context.s3_client:
        context.s3_client.close()



async def upload_to_s3(context: GlobalScraperContext, product_buffer): 
    # Determine which implementation of the upload_to_s3 function to call, based on scraper type.
    if context.scraper_type == 'ps':
        await upload_to_s3_ps(context, product_buffer)
    elif context.scraper_type == 'meta':
        await upload_to_s3_meta(context, product_buffer)



async def upload_to_s3_ps(context: GlobalScraperContext, product_buffer): 
    """
    Uploads a list of sanitized product dictionaries to S3 in JSONL format.
    For seen products (having metadata), the file is stored to "daily_pricing" folder.
    For unseen products (missing metadata), the file is stored to "datasets/retailer_daily_unseen" folder.
    After successfully saving to s3, product_buffer is cleared.
    On exceeding max_attempts, scraper is paused for a few minutes to save proxy wastage.
    """
    if not product_buffer:
        return  # Avoid uploading an empty list

    # Split products in two lists, seen products (having metadata) and unseen_products (missing metadata).
    unseen_products, seen_products = await split_unseen_seen_products(context, product_buffer)

    # Convert products to JSONL format
    jsonl_content_seen = "\n".join(json.dumps(product) for product in seen_products)
    jsonl_content_unseen = "\n".join(json.dumps(product) for product in unseen_products)
    
    # Generate content hashes for the filename
    content_hash_seen = hashlib.md5(jsonl_content_seen.encode('utf-8')).hexdigest()
    content_hash_unseen = hashlib.md5(jsonl_content_unseen.encode('utf-8')).hexdigest()
    
    # Get current UTC time
    now = datetime.now(timezone.utc)
    
    # Format the date as YYYY-MM-DD
    date_str = now.strftime('%Y-%m-%d')
    
    # Determine the current quarter number
    quarter = get_current_quarter_number(now)
    
    # Construct the S3 file paths
    file_name_seen = f"daily_pricing/{date_str}-{quarter}/{content_hash_seen}-{uuid.uuid4()}.jsonl"      # For products that have metadata.
    file_name_unseen = f"datasets/{context.retailer_name.lower()}_daily_unseen/{date_str}-{quarter}/{content_hash_unseen}-{uuid.uuid4()}.jsonl"    # For products with missing metadata.
    
    # Attempt to upload with retries
    max_attempts = 3
    for current_attempt in range(1, max_attempts+1):     # Hint: 1, 2, 3
        try:
            
            if seen_products:
                # Upload seen product data to s3 "daily_pricing" folder.
                context.s3_client.put_object(Bucket=context.s3_bucket_name, Key=file_name_seen, Body=jsonl_content_seen)
                context.logger.log_s3_upload(len(seen_products), file_name_seen, 'seen', context.proxy_ids[0])
            
            if unseen_products:
                # Upload unseen product data to s3 "datasets/reatailer_daily_unseen" folder.
                context.s3_client.put_object(Bucket=context.s3_bucket_name, Key=file_name_unseen, Body=jsonl_content_unseen)
                context.logger.log_s3_upload(len(unseen_products), file_name_unseen, 'unseen', context.proxy_ids[0])
            
            # After data is successfully uploaded to s3, clear product buffer. And break out of retry loop.
            product_buffer.clear()
            break
        except Exception as e:
            context.logger.log_processing_error(message=f'S3 upload attempt {current_attempt} failed: {e}', proxy_id=context.proxy_ids[0])
            if current_attempt != max_attempts:
                # Wait, then retry.
                await asyncio.sleep(3) 
            else:
                # If there is an error on uploading to s3 after max_attempts, pause thread for 10 minutes, to save proxy wastage, then return.
                context.logger.log_processing_error(message=f"Failed to upload to s3 after {max_attempts} attempts.\nPausing worker for 10 minutes to save proxy wastage.", proxy_id=context.proxy_ids[0])
                await asyncio.sleep(10*60)      



async def upload_to_s3_meta(context: GlobalScraperContext, product_buffer): 
    """
    Uploads a list of sanitized product dictionaries to S3 in JSONL format.
    The file is saved to "daily_pricing" folder.
    After successfully saving to s3, product_buffer is cleared.
    On exceeding max_attempts, scraper is paused for a few minutes to save proxy wastage.
    """
    if not product_buffer:
        return  # Avoid uploading an empty list

    # Convert products to JSONL format
    jsonl_content = "\n".join(json.dumps(product) for product in product_buffer)
    
    # Generate content hash for the filename
    content_hash = hashlib.md5(jsonl_content.encode('utf-8')).hexdigest()
    
    # Get current UTC time
    now = datetime.now(timezone.utc)
    
    # Format the date as YYYY-MM-DD
    date_str = now.strftime('%Y-%m-%d')
    
    # Determine the current quarter number
    quarter = get_current_quarter_number(now)
    
    # Construct the S3 file paths
    file_name = f"murtaza2025/metadata/{context.retailer_name.lower()}_metadata/{date_str}-{quarter}/{content_hash}-{uuid.uuid4()}.jsonl"
    
    # Attempt to upload with retries
    max_attempts = 3
    for current_attempt in range(1, max_attempts+1):     # Hint: 1, 2, 3
        try:
           
            if product_buffer:
                # Upload file to s3.
                context.s3_client.put_object(Bucket=context.s3_bucket_name, Key=file_name, Body=jsonl_content)
                context.logger.log_s3_upload(len(product_buffer), file_name, 'seen', context.proxy_ids[0])

                # Add product URLs to Redis set "retailer_seen_urls", since we now have metadata for these products.
                seen_product_urls = [product['product_url'] for product in product_buffer]
                await context.redis_client.sadd(context.redis_seen_products_key, *seen_product_urls) 
                           
            # After data is successfully uploaded to s3, clear product buffer. And break out of retry loop.
            product_buffer.clear()
            break
        except Exception as e:
            context.logger.log_processing_error(message=f'S3 upload attempt {current_attempt} failed: {e}', proxy_id=context.proxy_ids[0])
            if current_attempt != max_attempts:
                # Wait, then retry.
                await asyncio.sleep(3) 
            else:
                # If there is an error on uploading to s3 after max_attempts, pause thread for 10 minutes, to save proxy wastage, then return.
                context.logger.log_processing_error(message=f"Failed to upload to s3 after {max_attempts} attempts.\nPausing worker for 10 minutes to save proxy wastage.", proxy_id=context.proxy_ids[0])
                await asyncio.sleep(10*60)                      
