from .context import GlobalScraperContext



# Function to load proxies from Redis
class Proxy():
    def __init__(self, proxy_id, url: str):
        self.proxy_id = proxy_id
        self.http_url = url
        if '@' in url:  # Username/password authentication. 
            proxy_string = url.split('//')[1] 
            self.ip = proxy_string.split('@')[1].split(':')[0]
            self.port = proxy_string.split('@')[1].split(':')[1]
            self.username = proxy_string.split('@')[0].split(':')[0]
            self.password = proxy_string.split('@')[0].split(':')[1]
        else:         # No authentication (whitelisted)
            proxy_string = url.split('//')[1]
            self.ip = proxy_string.split(':')[0]
            self.port = proxy_string.split(':')[1]
            self.username = None
            self.password = None


async def load_proxies(context: GlobalScraperContext):
    try:
        proxies = []
        for proxy_id in context.proxy_ids:
            ip_list = await context.redis_client.smembers(proxy_id)
            ip_list = [p.decode() for p in ip_list]
            for ip in ip_list:
                proxy_url = 'http://' + ip
                proxy = Proxy(proxy_id, proxy_url)
                proxies.append(proxy)
        
        context.proxies_list = proxies
        context.logger.log_info(f'Loaded {len(proxies)} proxies from redis.')
    
    except Exception as e:
        context.logger.log_processing_error(f'Error loading proxies: {e}', proxy_id=context.proxy_ids[0])
        return []