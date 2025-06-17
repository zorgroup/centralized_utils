import io
import json
import logging
from pathlib import Path
from centralized_utils.logger_v1 import LogController



def log_success():
    print('\n\n\n\n\n\n\n\nsuccess:')
    
    LOG_CONTROLLER.log_request(
        sanitization_rate=60.5, 
        response_time_ms=3532, 
        status=200, 
        error_msg=None, 
        proxy_id='prox-4a-proxyrotator-100thread:datacenter-worldwide_ipv4', 
        urls=['https://www.retailer.com/product1', 'https://www.retailer.com/product2']
    )
    
    print('----------------------------------------------------------------------------------------------------------')
    print('----------------------------------------------------------------------------------------------------------')
    print('----------------------------------------------------------------------------------------------------------')



def log_proxy_issue():
    print('\n\n\n\n\n\n\n\nproxy_issue:')
    
    LOG_CONTROLLER.log_request(
        sanitization_rate=0.0, 
        response_time_ms=3532, 
        status=403, 
        error_msg=None, 
        proxy_id='prox-4a-proxyrotator-100thread:datacenter-worldwide_ipv4', 
        urls=['https://www.retailer.com/product1']
    )
    
    print('----------------------------------------------------------------------------------------------------------')
    print('----------------------------------------------------------------------------------------------------------')
    print('----------------------------------------------------------------------------------------------------------')



def log_scraper_issue():
    print('\n\n\n\n\n\n\n\nscraper_issue:')
    
    LOG_CONTROLLER.log_request(
        sanitization_rate=0.0, 
        response_time_ms=3532, 
        status=None, 
        error_msg='error in extracting product data', 
        proxy_id='prox-4a-proxyrotator-100thread:datacenter-worldwide_ipv4', 
        urls=['https://www.retailer.com/product1', 'https://www.retailer.com/product2']
    )
    
    print('----------------------------------------------------------------------------------------------------------')
    print('----------------------------------------------------------------------------------------------------------')
    print('----------------------------------------------------------------------------------------------------------')



def log_processing_error():
    print('\n\n\n\n\n\n\n\nprocessing_error:')
    
    LOG_CONTROLLER.log_processing_error(
        message='Error connecting to Redis.'
    )
    
    print('----------------------------------------------------------------------------------------------------------')
    print('----------------------------------------------------------------------------------------------------------')
    print('----------------------------------------------------------------------------------------------------------')



def log_s3_upload(product_buffer):
    print('\n\n\n\n\n\n\n\ns3_upload:')
    
    LOG_CONTROLLER.log_s3_upload(
        product_count=len(product_buffer), 
        file_name='ab14fed3413153c324b.jsonl', 
        products_type='seen'
    )
    
    print('----------------------------------------------------------------------------------------------------------')
    print('----------------------------------------------------------------------------------------------------------')
    print('----------------------------------------------------------------------------------------------------------')



def log_products(products):
    print('\n\n\n\n\n\n\n\nproducts:')
    
    LOG_CONTROLLER.log_products(
        products=products, 
        proxy_id='prox-4a-proxyrotator-100thread:datacenter-worldwide_ipv4'
    )
    
    print('----------------------------------------------------------------------------------------------------------')
    print('----------------------------------------------------------------------------------------------------------')
    print('----------------------------------------------------------------------------------------------------------')



def get_log_as_string(func, *args, **kwargs):
    # Create a string stream to capture logs
    log_capture = io.StringIO()

    # Create a logging handler that writes to the string stream
    ch = logging.StreamHandler(log_capture)
    ch.setLevel(logging.DEBUG)

    # Get the root logger or any specific logger you expect the function to use
    logger = logging.getLogger()
    logger.addHandler(ch)

    # Optionally, preserve and then change the log level
    old_level = logger.level
    logger.setLevel(logging.DEBUG)

    try:
        # Call the function (that logs something)
        func(*args, **kwargs)
    finally:
        # Always remove our handler and reset the level after capturing
        logger.removeHandler(ch)
        logger.setLevel(old_level)

    # Get the log output
    captured_sting = log_capture.getvalue()

    # Extract EMF log from captured string:
    lines = captured_sting.split('\n')
    for line in lines:
        if '_aws' in line:
            return line
        


def main():
    # Initialize the log controller
    global LOG_CONTROLLER
    LOG_CONTROLLER = LogController(scraper_name='ws_retailer_m1')

    # Create dummy products.
    dummy_products = [
        {
            'price': 20.0,
            'currency': 'USD',
            'in_stock': True,
            'product_url': 'https://www.retailer.com/product1'
        },
        {
            'price': 15.0,
            'currency': 'USD',
            'in_stock': False,
            'product_url': 'https://www.retailer.com/product2'
        },
        {
            'price': 10.0,
            'currency': 'USD',
            'in_stock': True,
            'product_url': 'https://www.retailer.com/product3'
        },
        {
            'price': 7.0,
            'currency': 'USD',
            'in_stock': True,
            'product_url': 'https://www.retailer.com/product4'
        },
        {
            'price': 8.0,
            'currency': 'USD',
            'in_stock': False,
            'product_url': 'https://www.retailer.com/product5'
        },
    ]

    # Generate logs.
    success_log = get_log_as_string(log_success)
    proxy_issue_log = get_log_as_string(log_proxy_issue)
    scraper_issue_log = get_log_as_string(log_scraper_issue)
    processing_error_log = get_log_as_string(log_processing_error)
    products_log = get_log_as_string(log_products, dummy_products[0:3])
    s3_upload_log = get_log_as_string(log_s3_upload, dummy_products)


    # Write to json files with pretty indentation.
    samples_dir = Path(__file__).parent
    with open(samples_dir / 'success.json', 'w', encoding='utf-8') as f:
        json.dump(json.loads(success_log), f, indent=4)
    with open(samples_dir / 'proxy_issue.json', 'w', encoding='utf-8') as f:
        json.dump(json.loads(proxy_issue_log), f, indent=4)
    with open(samples_dir / 'scraper_issue.json', 'w', encoding='utf-8') as f:
        json.dump(json.loads(scraper_issue_log), f, indent=4)
    with open(samples_dir /'processing_error.json', 'w', encoding='utf-8') as f:
        json.dump(json.loads(processing_error_log), f, indent=4)
    with open(samples_dir / 'products.json', 'w', encoding='utf-8') as f:
        json.dump(json.loads(products_log), f, indent=4)
    with open(samples_dir / 's3_upload.json', 'w', encoding='utf-8') as f:
        json.dump(json.loads(s3_upload_log), f, indent=4)


    print('\n\nSample logs written to files.\n')



if __name__ == '__main__':
    main()


