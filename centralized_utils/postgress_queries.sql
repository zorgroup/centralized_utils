
-- Create copy of Table scrapers_configuration
CREATE TABLE IF NOT EXISTS scrapers.scrapers_configuration_copy
(
    scraper_name character varying(30) NOT NULL,
    concurrency integer NOT NULL,
    request_delay integer,
    urls_per_batch integer NOT NULL,
    s3_bulk_size integer NOT NULL,
    estimate_completion_days integer NOT NULL,
    proxy_enabled boolean NOT NULL,
    stats_enabled boolean,
    stats_counter integer,
    bucket_name character varying(255) NOT NULL,
    redis_host character varying(255) NOT NULL,
    redis_port integer NOT NULL DEFAULT 6379,
    source_key_temp character varying(50) NOT NULL,
    scraper_state_key character varying(50) NOT NULL,
    api_keys json,
    proxy_ids character varying(255)[],
    container_state integer NOT NULL,
    source_key_master character varying(50) NOT NULL,
    seen_products_key character varying(50),
    CONSTRAINT scrapers_configuration_copy_pkey PRIMARY KEY (scraper_name)
);


-- Insert rows into copy of table scrapers_configuration
INSERT INTO scrapers.scrapers_configuration_copy VALUES ('ws_walmart_m4', 5, NULL, 5, 1000, 7, true, true, 30, 'webscraperowaisbucket', 'redispublicvalkey-zxe3ge.serverless.usw2.cache.amazonaws.com', 6379, 'walmart_m4_src_temp', 'walmart_m4_scraping_state', NULL, '{prox-4a-proxyrotator-100thread:datacenter-us-ipv4}', 1, 'walmart_m4_src_master', 'walmart_seen_products');
INSERT INTO scrapers.scrapers_configuration_copy VALUES ('ws_fleetpride_m1', 1, 10, 1000, 1000, 3, true, true, 10, 'webscraperowaisbucket', 'redispublicvalkey-zxe3ge.serverless.usw2.cache.amazonaws.com', 6379, 'fleetpride_m1_src_temp', 'fleetpride_m1_scraping_state', NULL, '{prox-4a-proxyrotator-100thread:datacenter-worldwide_ipv4}', 1, 'fleetpride_m1_src_master', NULL);
INSERT INTO scrapers.scrapers_configuration_copy VALUES ('ws_finditparts_m4', 5, NULL, 5, 500, 18, true, true, 30, 'webscraperowaisbucket', 'redispublicvalkey-zxe3ge.serverless.usw2.cache.amazonaws.com', 6379, 'finditparts_m4_src_temp', 'finditparts_m4_scraping_state', NULL, '{prox-5a-geonode:residential-usa-rotating}', 1, 'finditparts_m4_src_master', 'finditparts_seen_products');
INSERT INTO scrapers.scrapers_configuration_copy VALUES ('ws_oreilly_m9', 7, NULL, 7, 500, 3, true, true, 100, 'webscraperowaisbucket', 'redispublicvalkey-zxe3ge.serverless.usw2.cache.amazonaws.com', 6379, 'oreilly_m9_src_temp', 'oreilly_m9_scraping_state', NULL, '{prox-4a-proxyrotator-100thread:datacenter-us-ipv4}', 1, 'oreilly_m9_src_master', NULL);
INSERT INTO scrapers.scrapers_configuration_copy VALUES ('ws_autozone_m2', 1, 20, 50, 10000, 6, true, true, 10, 'webscraperowaisbucket', 'redispublicvalkey-zxe3ge.serverless.usw2.cache.amazonaws.com', 6379, 'autozone_m2_src_temp', 'autozone_m2_scraping_state', NULL, '{prox-4a-proxyrotator-100thread:datacenter-worldwide_ipv4}', 1, 'autozone_m2_src_master', NULL);
INSERT INTO scrapers.scrapers_configuration_copy VALUES ('ws_rockauto_m2', 1, 10, 100, 1000, 18, true, true, 30, 'webscraperowaisbucket', 'redispublicvalkey-zxe3ge.serverless.usw2.cache.amazonaws.com', 6379, 'rockauto_m2_src_temp', 'rockauto_m2_scraping_state', NULL, '{prox-5a-geonode:residential-usa-rotating}', 1, 'rockauto_m2_src_master', 'rockauto_seen_products');
INSERT INTO scrapers.scrapers_configuration_copy VALUES ('ws_amazon_m1', 15, NULL, 15, 1000, 10, true, true, 60, 'webscraperowaisbucket', 'redispublicvalkey-zxe3ge.serverless.usw2.cache.amazonaws.com', 6379, 'amazon_m1_src_temp', 'amazon_m1_scraping_state', NULL, '{prox-5a-geonode:residential-usa-rotating}', 1, 'amazon_m1_src_master', NULL);
