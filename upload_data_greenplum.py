import pandas as pd
import hashlib
from datetime import datetime
from sqlalchemy import create_engine, text

username = 'student12'
password = ''
host = ''
port = ''
bd = ''

engine = create_engine(
    f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{bd}',
    connect_args={
        'application_name': 'data_loader',
        'connect_timeout': 10
    },
    pool_pre_ping=True
)

path = ''
df = pd.read_csv(path + "SampleSuperstore.csv")


def generate_hash(key):
    return hashlib.md5(str(key).encode()).hexdigest()


def create_business_keys(dataframe):
    dataframe['order_business_key'] = dataframe.index.astype(str)

    dataframe['product_business_key'] = dataframe['Category'] + '|' + dataframe['Sub-Category']

    dataframe['customer_business_key'] = dataframe['Segment'] + '|' + dataframe['City'] + '|' + dataframe['State']

    dataframe['location_business_key'] = dataframe['City'] + '|' + dataframe['State'] + '|' + dataframe[
        'Postal Code'].astype(str)

    return dataframe


def create_hash_keys(dataframe):
    dataframe['order_hash_key'] = dataframe['order_business_key'].apply(generate_hash)
    dataframe['product_hash_key'] = dataframe['product_business_key'].apply(generate_hash)
    dataframe['customer_hash_key'] = dataframe['customer_business_key'].apply(generate_hash)
    dataframe['location_hash_key'] = dataframe['location_business_key'].apply(generate_hash)

    dataframe['order_link_hash_key'] = (
            dataframe['order_hash_key'] +
            dataframe['customer_hash_key'] +
            dataframe['product_hash_key'] +
            dataframe['location_hash_key']
    ).apply(generate_hash)

    return dataframe


def create_tables():
    create_table_queries = [

        f"""
        CREATE TABLE IF NOT EXISTS {username}.hub_order (
            order_hash_key TEXT PRIMARY KEY,
            order_business_key TEXT,
            load_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            record_source TEXT
        ) DISTRIBUTED BY (order_hash_key);
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {username}.hub_product (
            product_hash_key TEXT PRIMARY KEY,
            product_business_key TEXT,
            load_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            record_source TEXT
        ) DISTRIBUTED BY (product_hash_key);
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {username}.hub_customer (
            customer_hash_key TEXT PRIMARY KEY,
            customer_business_key TEXT,
            load_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            record_source TEXT
        ) DISTRIBUTED BY (customer_hash_key);
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {username}.hub_location (
            location_hash_key TEXT PRIMARY KEY,
            location_business_key TEXT,
            load_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            record_source TEXT
        ) DISTRIBUTED BY (location_hash_key);
        """,

        f"""
        CREATE TABLE IF NOT EXISTS {username}.link_order (
            order_link_hash_key TEXT PRIMARY KEY,
            order_hash_key TEXT,
            customer_hash_key TEXT,
            product_hash_key TEXT,
            location_hash_key TEXT,
            load_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            record_source TEXT
        ) DISTRIBUTED BY (order_link_hash_key);
        """,

        f"""
        CREATE TABLE IF NOT EXISTS {username}.sat_order_details (
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
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {username}.sat_product_details (
            product_hash_key TEXT,
            load_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            category TEXT,
            sub_category TEXT,
            record_source TEXT,
            PRIMARY KEY (product_hash_key, load_dttm)
        ) DISTRIBUTED BY (product_hash_key);
        """,
        f"""
       CREATE TABLE IF NOT EXISTS {username}.sat_customer_details (
            customer_hash_key TEXT,
            load_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            segment TEXT,
            record_source TEXT,
            PRIMARY KEY (customer_hash_key, load_dttm)
        ) DISTRIBUTED BY (customer_hash_key);
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {username}.sat_location_details (
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
        """
    ]

    print("Создание таблиц...")
    for query in create_table_queries:
        try:
            with engine.connect() as conn:
                conn.execute(text(query))
                conn.commit()
            print("✓ Таблица создана успешно")
        except Exception as e:
            print(f"✗ Ошибка при создании таблицы: {e}")


def load_data_to_tables(dataframe):
    current_timestamp = datetime.now()
    record_source = "SampleSuperstore.csv"

    try:
        with engine.connect() as conn:
            hub_order_data = dataframe[['order_hash_key', 'order_business_key']].drop_duplicates()
            hub_order_data['load_dttm'] = current_timestamp
            hub_order_data['record_source'] = record_source
            hub_order_data.to_sql('hub_order', conn, schema=f'{username}', if_exists='replace', index=False)
            print(f"✓ Загружено {len(hub_order_data)} записей в hub_order")

            hub_product_data = dataframe[['product_hash_key', 'product_business_key']].drop_duplicates()
            hub_product_data['load_dttm'] = current_timestamp
            hub_product_data['record_source'] = record_source
            hub_product_data.to_sql('hub_product', conn, schema=f'{username}', if_exists='replace', index=False)
            print(f"✓ Загружено {len(hub_product_data)} записей в hub_product")

            hub_customer_data = dataframe[['customer_hash_key', 'customer_business_key']].drop_duplicates()
            hub_customer_data['load_dttm'] = current_timestamp
            hub_customer_data['record_source'] = record_source
            hub_customer_data.to_sql('hub_customer', conn, schema=f'{username}', if_exists='replace', index=False)
            print(f"✓ Загружено {len(hub_customer_data)} записей в hub_customer")

            hub_location_data = dataframe[['location_hash_key', 'location_business_key']].drop_duplicates()
            hub_location_data['load_dttm'] = current_timestamp
            hub_location_data['record_source'] = record_source
            hub_location_data.to_sql('hub_location', conn, schema=f'{username}', if_exists='replace', index=False)
            print(f"✓ Загружено {len(hub_location_data)} записей в hub_location")

            link_order_data = dataframe[[
                'order_link_hash_key', 'order_hash_key', 'customer_hash_key',
                'product_hash_key', 'location_hash_key'
            ]].drop_duplicates()
            link_order_data['load_dttm'] = current_timestamp
            link_order_data['record_source'] = record_source
            link_order_data.to_sql('link_order', conn, schema=f'{username}', if_exists='replace', index=False)
            print(f"✓ Загружено {len(link_order_data)} записей в link_order")

            sat_order_details_data = dataframe[[
                'order_link_hash_key', 'Sales', 'Quantity', 'Discount',
                'Profit', 'Ship Mode'
            ]].copy()
            sat_order_details_data = sat_order_details_data.rename(columns={'Ship Mode': 'ship_mode'})
            sat_order_details_data['load_dttm'] = current_timestamp
            sat_order_details_data['record_source'] = record_source
            sat_order_details_data.to_sql('sat_order_details', conn, schema=f'{username}', if_exists='replace',
                                          index=False)
            print(f"✓ Загружено {len(sat_order_details_data)} записей в sat_order_details")

            sat_product_details_data = dataframe[[
                'product_hash_key', 'Category', 'Sub-Category'
            ]].drop_duplicates()
            sat_product_details_data = sat_product_details_data.rename(columns={
                'Category': 'category',
                'Sub-Category': 'sub_category'
            })
            sat_product_details_data['load_dttm'] = current_timestamp
            sat_product_details_data['record_source'] = record_source
            sat_product_details_data.to_sql('sat_product_details', conn, schema=f'{username}', if_exists='replace',
                                            index=False)
            print(f"✓ Загружено {len(sat_product_details_data)} записей в sat_product_details")

            sat_customer_details_data = dataframe[['customer_hash_key', 'Segment']].drop_duplicates()
            sat_customer_details_data = sat_customer_details_data.rename(columns={'Segment': 'segment'})
            sat_customer_details_data['load_dttm'] = current_timestamp
            sat_customer_details_data['record_source'] = record_source
            sat_customer_details_data.to_sql('sat_customer_details', conn, schema=f'{username}', if_exists='replace',
                                             index=False)
            print(f"✓ Загружено {len(sat_customer_details_data)} записей в sat_customer_details")

            sat_location_details_data = dataframe[[
                'location_hash_key', 'City', 'State', 'Postal Code', 'Region', 'Country'
            ]].drop_duplicates()
            sat_location_details_data = sat_location_details_data.rename(columns={
                'City': 'city',
                'State': 'state',
                'Postal Code': 'postal_code',
                'Region': 'region',
                'Country': 'country'
            })
            sat_location_details_data['load_dttm'] = current_timestamp
            sat_location_details_data['record_source'] = record_source
            sat_location_details_data.to_sql('sat_location_details', conn, schema=f'{username}', if_exists='replace',
                                             index=False)
            print(f"✓ Загружено {len(sat_location_details_data)} записей в sat_location_details")

            conn.commit()

    except Exception as e:
        print(f"✗ Ошибка при загрузке данных: {e}")
        raise


def verify_data_loading():
    verification_queries = {
        'hub_order': f"SELECT COUNT(*) FROM {username}.hub_order",
        'hub_product': f"SELECT COUNT(*) FROM {username}.hub_product",
        'hub_customer': f"SELECT COUNT(*) FROM {username}.hub_customer",
        'hub_location': f"SELECT COUNT(*) FROM {username}.hub_location",
        'link_order': f"SELECT COUNT(*) FROM {username}.link_order",
        'sat_order_details': f"SELECT COUNT(*) FROM {username}.sat_order_details",
        'sat_product_details': f"SELECT COUNT(*) FROM {username}.sat_product_details",
        'sat_customer_details': f"SELECT COUNT(*) FROM {username}.sat_customer_details",
        'sat_location_details': f"SELECT COUNT(*) FROM {username}.sat_location_details"
    }

    print("\nПроверка загрузки данных:")
    print("-" * 40)

    with engine.connect() as conn:
        for table_name, query in verification_queries.items():
            try:
                result = conn.execute(text(query))
                count = result.scalar()
                print(f"✓ {table_name}: {count} записей")
            except Exception as e:
                print(f"✗ Ошибка при проверке {table_name}: {e}")


def main():
    print("Начало обработки данных...")
    print(f"Размер исходного датасета: {len(df)} строк")

    df_processed = create_business_keys(df.copy())
    df_processed = create_hash_keys(df_processed)

    print("✓ Бизнес-ключи и хеш-ключи сгенерированы")

    create_tables()

    load_data_to_tables(df_processed)

    verify_data_loading()

    print("\n✅ Все операции завершены успешно!")
    print(f"📊 Исходных записей: {len(df)}")
    print(f"🏗️  Данные загружены в схему {username}")
    return df_processed


if __name__ == "__main__":
    df_processed = main()
