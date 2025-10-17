CREATE TABLE IF NOT EXISTS student12.sat_order_details (
    order_link_hash_key TEXT,
    load_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sales NUMERIC,
    quantity INTEGER,
    discount NUMERIC,
    profit NUMERIC,
    ship_mode TEXT,
    record_source TEXT,
    PRIMARY KEY (order_link_hash_key, load_dttm)
) DISTRIBUTED BY (order_link_hash_key);

CREATE TABLE IF NOT EXISTS student12.sat_product_details (
    product_hash_key TEXT,
    load_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    category TEXT,
    sub_category TEXT,
    record_source TEXT,
    PRIMARY KEY (product_hash_key, load_dttm)
) DISTRIBUTED BY (product_hash_key);

CREATE TABLE IF NOT EXISTS student12.sat_customer_details (
    customer_hash_key TEXT,
    load_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    segment TEXT,
    record_source TEXT,
    PRIMARY KEY (customer_hash_key, load_dttm)
) DISTRIBUTED BY (customer_hash_key);

CREATE TABLE IF NOT EXISTS student12.sat_location_details (
    location_hash_key TEXT,
    load_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    region TEXT,
    country TEXT,
    record_source TEXT,
    PRIMARY KEY (location_hash_key, load_dttm)
) DISTRIBUTED BY (location_hash_key);
