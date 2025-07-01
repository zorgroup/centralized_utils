import os
import redis
import random
import asyncio
from curl_cffi import requests
import traceback
from pathlib import Path
from dotenv import load_dotenv


def get_redis_client():
    print('Connecting to redis...')
    redis_client = redis.Redis(
        host=os.getenv('REDIS_HOST'),
        port=os.getenv('REDIS_PORT'),
        # password=DB_PASS,
        # db=REDIS_DB_NUMBER,
        ssl=True,
    )
    return redis_client



def load_proxy_subscriptions(redis_client):    
    proxy_subscriptions = {}      # Keys are subsctiption ids, values are list of IPs.
    
    # Get all subscription ids stored on redis.
    _, keys = redis_client.scan(cursor=0, match="prox*", count=5000)
    subscription_ids = [key.decode('utf-8') for key in keys]
    print(F'Found {len(subscription_ids)} subscription IDs.')

    # Extract IPs for each subcription ID.
    for id in subscription_ids:
        ip_list = redis_client.smembers(id)
        ip_list = [p.decode('utf-8') for p in ip_list]
        proxy_subscriptions[id] = ip_list
        print(f'Loaded: {id}')

    return proxy_subscriptions




async def fetch_ip_info(session, gateway_url, semaphore):
    async with semaphore:
        while True:    # Keep retrying until response is received.
            try:               
                response = await session.get(
                    f'https://ipinfo.io/json?val={random.randint(1,10000)}', 
                    proxy=gateway_url, 
                    timeout=15, 
                    verify=False, 
                    impersonate='chrome131'
                )
                if response.status_code != 200:
                    raise Exception(f'Invalid status: {response.status_code}')
                data = response.json()
                ip =  data['ip']
                subnet = '.'.join(data['ip'].split('.')[0:3]),
                country = data['country']
                return ip, subnet, country
            except Exception as e:
                print(f'\nERROR ----------- {gateway_url}')
                #print(str(e))
                #traceback.print_exc()
                await asyncio.sleep(5)
        


async def evaluate_proxy_subscription(subscription_id: str, ip_gateways: list, num_requests_to_test: int, concurrency: int):
    # Create stats

    request_count = 0
    ip_pool = set()
    subnet_pool = set()
    ip_country_counters = {
        'US': 0,
        'Worldwide': 0
    }


    print(f'\n\nEvaluating proxy id: {subscription_id}')
    async with requests.AsyncSession() as session:
        tasks = []
        semaphore = asyncio.Semaphore(concurrency)
        for _ in range(num_requests_to_test):
            gateway_url = f'http://' + random.choice(ip_gateways)
            tasks.append(fetch_ip_info(session, gateway_url, semaphore))

        for i, task in enumerate(asyncio.as_completed(tasks), 1):
                ip, subnet, country = await task
                
                # Increment reqesut count.
                request_count += 1

                # Increment country counts (before saving ip).
                if ip not in ip_pool:
                    if country == 'US':
                        ip_country_counters['US'] += 1
                    else:
                        ip_country_counters['Worldwide'] += 1

                # Save ip and subnet.
                ip_pool.add(ip)
                subnet_pool.add(subnet)

                # Print results periodically.
                if request_count % 10 == 0:
                    print(f'\nProxy ID:        {subscription_id}')
                    print(f"Request Count:   {request_count:,}")
                    print(f"Unique IPs:      {len(ip_pool):,}")
                    print(f"Unique Subnets:  {len(subnet_pool):,}")
                    print(f"IP Countries:    {dict(ip_country_counters)}")
           

    # Print results at end of evaluation.
    print("\n\n------------------------------------------------------ Proxy Final Summary ------------------------------------------------------")
    print(f'Proxy ID:        {subscription_id}')
    print(f"Request Count:   {request_count:,}")
    print(f"Unique IPs:      {len(ip_pool):,}")
    print(f"Unique Subnets:  {len(subnet_pool):,}")
    print(f"IP Countries:    {dict(ip_country_counters)}\n")
    print("---------------------------------------------------------------------------------------------------------------------------------")

    # Write results to file.
    with open('proxy_evaluation_results.txt', 'a', encoding='utf-8') as f:
        f.write(f'Proxy ID:        {subscription_id}\n')
        f.write(f"Request Count:   {request_count:,}\n")
        f.write(f"Unique IPs:      {len(ip_pool):,}\n")
        f.write(f"Unique Subnets:  {len(subnet_pool):,}\n")
        f.write(f"IP Countries:    {dict(ip_country_counters)}\n\n\n\n")




async def main():

    # Create results file.
    with open('proxy_evaluation_results.txt', 'w') as f:
        f.write('')

    # Load all proxy subscriptions from redis.
    load_dotenv()
    redis_client = get_redis_client()
    proxy_subscriptions = load_proxy_subscriptions(redis_client)
    redis_client.close()


    input("\nPlease disconnect OpenVPN (some proxies don't work with it), then press enter:")         # Geonode does not work with OpenVPN.

    # Evaluate each proxy subscription.
    tasks = []
    for subscription_id, ip_gateways in proxy_subscriptions.items():
        task = asyncio.create_task(
            evaluate_proxy_subscription(
                subscription_id=subscription_id, 
                ip_gateways=ip_gateways,
                num_requests_to_test=10_000,
                concurrency=5
            )
        )
        tasks.append(task)

    await asyncio.gather(*tasks)
        
    print('\nAll results saved!')

if __name__ == '__main__':
    asyncio.run(main())
    