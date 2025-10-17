import pandas as pd
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


def check_data_quality():
    checks = [
        ("Уникальные заказы", f"SELECT COUNT(DISTINCT order_hash_key) FROM {username}.hub_order"),
        ("Уникальные продукты", f"SELECT COUNT(DISTINCT product_hash_key) FROM {username}.hub_product"),
        ("Уникальные клиенты", f"SELECT COUNT(DISTINCT customer_hash_key) FROM {username}.hub_customer"),
        ("Уникальные локации", f"SELECT COUNT(DISTINCT location_hash_key) FROM {username}.hub_location"),

        ("Связи заказов", f"SELECT COUNT(DISTINCT order_link_hash_key) FROM {username}.link_order"),

        ("Детали заказов", f"SELECT COUNT(*) FROM {username}.sat_order_details"),
        ("Детали продуктов", f"SELECT COUNT(*) FROM {username}.sat_product_details"),
        ("Детали клиентов", f"SELECT COUNT(*) FROM {username}.sat_customer_details"),
        ("Детали локаций", f"SELECT COUNT(*) FROM {username}.sat_location_details"),
    ]

    print("Проверка целостности данных:")
    print("=" * 50)

    with engine.connect() as conn:
        for check_name, query in checks:
            try:
                result = conn.execute(text(query))
                count = result.scalar()
                print(f"✓ {check_name}: {count}")
            except Exception as e:
                print(f"✗ Ошибка при проверке {check_name}: {e}")


def sample_data_preview():
    tables = [
        'hub_order', 'hub_product', 'hub_customer', 'hub_location',
        'link_order', 'sat_order_details', 'sat_product_details',
        'sat_customer_details', 'sat_location_details'
    ]

    print("\nПримеры данных из таблиц:")
    print("=" * 50)

    with engine.connect() as conn:
        for table in tables:
            try:
                query = f"SELECT * FROM {username}.{table} LIMIT 2"
                result = conn.execute(text(query))
                rows = result.fetchall()
                print(f"\n{table}: {len(rows)} примеров записей")
                for row in rows:
                    print(f"  {row}")
            except Exception as e:
                print(f"✗ Ошибка при чтении {table}: {e}")


if __name__ == "__main__":
    check_data_quality()
    sample_data_preview()
