CREATE TABLE IF NOT EXISTS student12.link_order (
    order_link_hash_key TEXT PRIMARY KEY,
    order_hash_key TEXT,
    customer_hash_key TEXT,
    product_hash_key TEXT,
    location_hash_key TEXT,
    load_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_source TEXT
) DISTRIBUTED BY (order_link_hash_key);