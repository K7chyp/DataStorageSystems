# DataStorageSystems


<!-- лог upload_data_greenplum.py -->  
Начало обработки данных...
Размер исходного датасета: 9994 строк
✓ Бизнес-ключи и хеш-ключи сгенерированы
Создание таблиц...
✓ Таблица создана успешно
✓ Таблица создана успешно
✓ Таблица создана успешно
✓ Таблица создана успешно
✓ Таблица создана успешно
✓ Таблица создана успешно
✓ Таблица создана успешно
✓ Таблица создана успешно
✓ Таблица создана успешно
✓ Загружено 9994 записей в hub_order
✓ Загружено 17 записей в hub_product
✓ Загружено 1175 записей в hub_customer
✓ Загружено 632 записей в hub_location
✓ Загружено 9994 записей в link_order
✓ Загружено 9994 записей в sat_order_details
✓ Загружено 17 записей в sat_product_details
✓ Загружено 1175 записей в sat_customer_details
✓ Загружено 632 записей в sat_location_details



<!-- лог check_uploaded_data.py -->  
Проверка загрузки данных:
----------------------------------------
✓ hub_order: 9994 записей
✓ hub_product: 17 записей
✓ hub_customer: 1175 записей
✓ hub_location: 632 записей
✓ link_order: 9994 записей
✓ sat_order_details: 9994 записей
✓ sat_product_details: 17 записей
✓ sat_customer_details: 1175 записей
✓ sat_location_details: 632 записей

✅ Все операции завершены успешно!
📊 Исходных записей: 9994

Проверка целостности данных:
==================================================
✓ Уникальные заказы: 9994
✓ Уникальные продукты: 17
✓ Уникальные клиенты: 1175
✓ Уникальные локации: 632
✓ Связи заказов: 9994
✓ Детали заказов: 9994
✓ Детали продуктов: 17
✓ Детали клиентов: 1175
✓ Детали локаций: 632

Примеры данных из таблиц:
==================================================

hub_order: 2 примеров записей
  ('cfcd208495d565ef66e7dff9f98764da', '0', datetime.datetime(2025, 10, 17, 18, 51, 37, 802390), 'SampleSuperstore.csv')
  ('c4ca4238a0b923820dcc509a6f75849b', '1', datetime.datetime(2025, 10, 17, 18, 51, 37, 802390), 'SampleSuperstore.csv')

hub_product: 2 примеров записей
  ('543fee57c28c7ebf8e0e54c24679906e', 'Office Supplies|Labels', datetime.datetime(2025, 10, 17, 18, 51, 37, 802390), 'SampleSuperstore.csv')
  ('3b1effbc068b1d8db79129726885a86d', 'Furniture|Tables', datetime.datetime(2025, 10, 17, 18, 51, 37, 802390), 'SampleSuperstore.csv')

hub_customer: 2 примеров записей
  ('83f2f6a5ef1a75d9ca43dade30633a7d', 'Consumer|Henderson|Kentucky', datetime.datetime(2025, 10, 17, 18, 51, 37, 802390), 'SampleSuperstore.csv')
  ('e9a96bbe972ef78ed6d4970cb8d507a1', 'Consumer|Los Angeles|California', datetime.datetime(2025, 10, 17, 18, 51, 37, 802390), 'SampleSuperstore.csv')

hub_location: 2 примеров записей
  ('dcd1c5a5b15b36dd080343901efbeaec', 'Henderson|Kentucky|42420', datetime.datetime(2025, 10, 17, 18, 51, 37, 802390), 'SampleSuperstore.csv')
  ('3b44ca9682a36de5cabe4e5de0efa20a', 'Los Angeles|California|90032', datetime.datetime(2025, 10, 17, 18, 51, 37, 802390), 'SampleSuperstore.csv')

link_order: 2 примеров записей
  ('073525e846497bf12350964af8d4c70c', 'c4ca4238a0b923820dcc509a6f75849b', '83f2f6a5ef1a75d9ca43dade30633a7d', 'fd82a1d5c21c9a11968af233a962cec1', 'dcd1c5a5b15b36dd080343901efbeaec', datetime.datetime(2025, 10, 17, 18, 51, 37, 802390), 'SampleSuperstore.csv')
  ('bfedb9d177c57767a84adb53e3cae92c', 'c81e728d9d4c2f636f067f89cc14862c', '8db66c5bc3c111c53522f720e13649e4', '543fee57c28c7ebf8e0e54c24679906e', '02668a803c4fe8eeab2503487b9ec817', datetime.datetime(2025, 10, 17, 18, 51, 37, 802390), 'SampleSuperstore.csv')

sat_order_details: 2 примеров записей
  ('073525e846497bf12350964af8d4c70c', 731.94, 3, 0.0, 219.582, 'Second Class', datetime.datetime(2025, 10, 17, 18, 51, 37, 802390), 'SampleSuperstore.csv')
  ('bfedb9d177c57767a84adb53e3cae92c', 14.62, 2, 0.0, 6.8714, 'Second Class', datetime.datetime(2025, 10, 17, 18, 51, 37, 802390), 'SampleSuperstore.csv')

sat_product_details: 2 примеров записей
  ('2fba7e64a1e2478776d4e60b7f776dfc', 'Furniture', 'Bookcases', datetime.datetime(2025, 10, 17, 18, 51, 37, 802390), 'SampleSuperstore.csv')
  ('fd82a1d5c21c9a11968af233a962cec1', 'Furniture', 'Chairs', datetime.datetime(2025, 10, 17, 18, 51, 37, 802390), 'SampleSuperstore.csv')

sat_customer_details: 2 примеров записей
  ('8db66c5bc3c111c53522f720e13649e4', 'Corporate', datetime.datetime(2025, 10, 17, 18, 51, 37, 802390), 'SampleSuperstore.csv')
  ('dd5baab306933452024bceff73c71926', 'Consumer', datetime.datetime(2025, 10, 17, 18, 51, 37, 802390), 'SampleSuperstore.csv')

sat_location_details: 2 примеров записей
  ('dcd1c5a5b15b36dd080343901efbeaec', 'Henderson', 'Kentucky', 42420, 'South', 'United States', datetime.datetime(2025, 10, 17, 18, 51, 37, 802390), 'SampleSuperstore.csv')
  ('3b44ca9682a36de5cabe4e5de0efa20a', 'Los Angeles', 'California', 90032, 'West', 'United States', datetime.datetime(2025, 10, 17, 18, 51, 37, 802390), 'SampleSuperstore.csv')
