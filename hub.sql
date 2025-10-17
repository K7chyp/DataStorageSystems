CREATE TABLE IF NOT EXISTS student12.hub_order (
    order_hash_key TEXT PRIMARY KEY,
    order_business_key TEXT,
    load_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_source TEXT
) DISTRIBUTED BY (order_hash_key);

CREATE TABLE IF NOT EXISTS student12.hub_product (
    product_hash_key TEXT PRIMARY KEY,
    product_business_key TEXT,
    load_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_source TEXT
) DISTRIBUTED BY (product_hash_key);

CREATE TABLE IF NOT EXISTS student12.hub_customer (
    customer_hash_key TEXT PRIMARY KEY,
    customer_business_key TEXT,
    load_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_source TEXT
) DISTRIBUTED BY (customer_hash_key);

CREATE TABLE IF NOT EXISTS student12.hub_location (
    location_hash_key TEXT PRIMARY KEY,
    location_business_key TEXT,
    load_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_source TEXT
) DISTRIBUTED BY (location_hash_key);