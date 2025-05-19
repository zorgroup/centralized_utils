def sanitize_data(raw: dict) -> dict:
    """Sanitize raw product data, ensuring required fields and types."""
    sanitized = {}
    price = raw.get('price')
    try:
        sanitized['price'] = float(price)
    except (TypeError, ValueError):
        sanitized['price'] = 0.0
    sanitized['name'] = raw.get('name', "")
    return sanitized
