import re
from datetime import datetime
from .context import GlobalScraperContext


def sanitize_products(context: GlobalScraperContext, dirty_products: list[dict]):
    # Determine which implementation of the sanitization function to call, based on scraper type.
    if context.scraper_type == 'ps':
        sanitized_products, sanitization_rate = sanitize_products_ps(context, dirty_products)
    elif context.scraper_type == 'meta':
        sanitized_products, sanitization_rate = sanitize_products_meta(context, dirty_products)

    return sanitized_products, sanitization_rate


# Function to sanitize product data, updating schema should be not done unless mudassir authorizes it.
def sanitize_products_ps(context: GlobalScraperContext, dirty_products: list[dict]):
    sanitized_products = []

    if not isinstance(dirty_products, list):
        raise ValueError(f"'dirty_products' must be of type 'list[dict]'. Got '{type(dirty_products)}' instead")
    
    for product in dirty_products:
        try:
            schema = {
                "product_url": {"type": str, "required":True},
                "retailer": {"type": str, "required":True},     
                "retailers_brand": {"type": str, "required":False},
                "retailers_mpn": {"type": str, "required":False},
                "sku": {"type": str, "required":False},
                "price": {"type": float, "required":True}, 
                "in_stock": {"type": bool, "required":True},
                "currency": {"type": str, "required":True},
                "scraperid": {"type": str, "required":True},
                "date_download": {"type": str, "required":True},
                "scrape_method": {"type": str, "required":True},
            }
            # Create sanitized dict to store processed values
            sanitized = {}
            # for each key/value in provided data validate its type, confirm its existence if required = True, and in the end format the fields as needed
            for field, rules in schema.items():
                value = product.get(field)
                require = rules.get("required")
                type_ = rules.get("type")
                # Check if required field exists
                if require == True and value in (None, "", "null", "undefined"):
                    # Edge case # 1 for price, if retailer is amazone and price is None, dont pass key/value any further
                    if field == 'price' and product['retailer'] == 'Amazon':
                        continue
                    # Edge case # 2 for price, if in_stock is false and price is None, dont pass key/value any further
                    elif field == 'price' and product['in_stock'] == False:
                        continue
                    # Edge case # 3. Currency can be None, if price is None, don't pass key/value any further.
                    elif field == 'currency' and product['price'] in (None, "", "null", "undefined"):
                        continue
                    # For anything else if required Field value is None, raise valueError
                    else:
                        raise ValueError(f"Required field '{field}' is missing.")
                # if value is None and required=False, dont add key/value to sanitize
                if require == False and value in (None, "", "null", "undefined"):
                    continue
                # Check if value matches the schema types
                if value is not None and not isinstance(value, type_):
                    raise ValueError(f"Field '{field}' must be of type {type_.__name__}")
                # Apply field-specific formatting
                if value is not None:
                    # Handle float truncation
                    if field == "price" and isinstance(value, float):
                        value = float(f"{value:.2f}")
                    elif field == "avg_rating" and isinstance(value, float):
                        value = float(f"{value:.1f}")
                    # Handle date formats
                    elif field == "scraperid":
                        try:
                            datetime.strptime(value, "%Y-%m-%d")
                        except ValueError:
                            raise ValueError(f"Field 'scraperid' must match format YYYY-MM-DD")
                    elif field == "date_download":
                        try:
                            datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
                        except ValueError:
                            raise ValueError(f"Field 'date_download' must match format YYYY-MM-DDTHH:MM:SS")
                    # Handle currency
                    elif field == "currency" and isinstance(value, str):
                        value = value.upper()
                        if value != "USD":
                            raise ValueError(f"Field 'Currency' must be USD")
                sanitized[field] = value
            sanitized_products.append(sanitized)
        except Exception as e:
            context.logger.log_info(f'Failed to sanitize product - {str(e)} Product: {product}')
            continue  # Continue with the next product.
    
    sanitization_rate = (len(sanitized_products) / len(dirty_products) * 100) if dirty_products else 0

    return sanitized_products, sanitization_rate




# Function to sanitize product data, updating schema should be not done unless mudassir authorizes it.
def sanitize_products_meta(context: GlobalScraperContext, dirty_products: list[dict]):
    sanitized_products = []

    if not isinstance(dirty_products, list):
        raise ValueError(f"'dirty_products' must be of type 'list[dict]'. Got '{type(dirty_products)}' instead")
    
    for product in dirty_products:
        try:
            schema = {
                "product_url": {"type": str, "required":True},
                "retailer": {"type": str, "required":True},     
                "retailers_brand": {"type": str, "required":True},
                "retailers_mpn": {"type": str, "required":True},
                "title": {"type": str, "required":True},
                "sku": {"type": str, "required":False},
                "avg_rating": {"type": float, "required":False},
                "number_of_reviews": {"type": int, "required":False},
                "price": {"type": float, "required":False}, 
                "in_stock": {"type": bool, "required":False},
                "images": {"type": list, "required":False},
                "description": {"type": str, "required":False},
                "currency": {"type": str, "required":False},
                "retailers_upc": {"type": list, "required":False},
                "scraperid": {"type": str, "required":True},
                "date_download": {"type": str, "required":True},
                "scrape_method": {"type": str, "required":True},
            }
            # regex patter for html tags
            cleanr = re.compile('<.*?>')
            # Create sanitized dict to store processed values
            sanitized = {}
            # for each key/value in provided data validate its type, confirm its existence if required = True, and in the end format the fields as needed
            for field, rules in schema.items():
                value = product.get(field)
                require = rules.get("required")
                type_ = rules.get("type")
                # Check if required field exists
                if require == True and value in (None, "", "null", "undefined"):
                    raise ValueError(f"Required field '{field}' is missing.")
                # if value is None and required=False, dont add key/value to sanitize
                if require == False and value in (None, "", "null", "undefined"):
                    continue
                # Check if value matches the schema types
                if value is not None and not isinstance(value, type_):
                    raise ValueError(f"Field '{field}' must be of type {type_.__name__}")
                # Apply field-specific formatting
                if value is not None:
                    # Handle description
                    if field == "description" and isinstance(value, str):
                        value = re.sub(cleanr, '', value)
                        if len(value) > 2000:
                            value = ' '.join(value.split())[:2000]
                    # Handle float truncation
                    elif field == "price" and isinstance(value, float):
                        value = float(f"{value:.2f}")
                    elif field == "avg_rating" and isinstance(value, float):
                        value = float(f"{value:.1f}")
                    # Handle date formats
                    elif field == "scraperid":
                        try:
                            datetime.strptime(value, "%Y-%m-%d")
                        except ValueError:
                            raise ValueError(f"Field 'scraperid' must match format YYYY-MM-DD")
                    elif field == "date_download":
                        try:
                            datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
                        except ValueError:
                            raise ValueError(f"Field 'date_download' must match format YYYY-MM-DDTHH:MM:SS")
                    # Handle images (remove duplicates)
                    elif field == "images" and isinstance(value, list):
                        value = list(dict.fromkeys(value))
                    # Handle currency
                    elif field == "currency" and isinstance(value, str):
                        value = value.upper()
                        if value != "USD":
                            raise ValueError(f"Field 'Currency' must be USD")
                sanitized[field] = value
            sanitized_products.append(sanitized)
        except Exception as e:
            context.logger.log_info(f'Failed to sanitize product - {str(e)} Product: {product}')
            continue  # Continue with the next product.
    
    sanitization_rate = (len(sanitized_products) / len(dirty_products) * 100) if dirty_products else 0

    return sanitized_products, sanitization_rate


