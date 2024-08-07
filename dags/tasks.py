from airflow.decorators import dag, task_group, task
from datetime import datetime, timedelta
import pandas as pd
from extract import get_brands, get_devices
from load import init_brands_table, init_devices_table, load_brands_to_db, load_devices_to_db


default_args = {
    "retries": 25,
    "retry_delay": timedelta(minutes=1),
}

@dag(
    'get_brands_devices',
    description='Get all brands and device data',
    schedule='@monthly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args
)
def get_brands_devices():
    @task
    def get_brands_task():
        brands_df = get_brands()
        return brands_df.to_json()  # Convert DataFrame to JSON for XCom

    @task
    def get_devices_task():
        devices_df = get_devices()
        return devices_df.to_json()  # Convert DataFrame to JSON for XCom

    @task
    def init_brands_table_task():
        init_brands_table()

    @task
    def init_devices_table_task():
        init_devices_table()

    @task
    def load_brands_to_db_task(brands_json):
        brands_df = pd.read_json(brands_json)  # Convert JSON back to DataFrame
        load_brands_to_db(brands_df)

    @task
    def load_devices_to_db_task(devices_json):
        devices_df = pd.read_json(devices_json)  # Convert JSON back to DataFrame
        load_devices_to_db(devices_df)

    @task_group
    def init_get_brands_devices():
        brands_json = get_brands_task()
        devices_json = get_devices_task()
        return brands_json, devices_json

    @task_group
    def init_brands_devices_table():
        init_brands_table_task()
        init_devices_table_task()

    @task_group
    def load_brands_devices(brands_json, devices_json):
        load_brands_to_db_task(brands_json)
        load_devices_to_db_task(devices_json)

    # Task dependencies
    brands_json, devices_json = init_get_brands_devices()
    init = init_brands_devices_table()
    load = load_brands_devices(brands_json, devices_json)

    init >> load

get_brands_devices()



# def get_device_details(key):
#     # TODO Divide these to multiple functions
#     r = 'https://script.google.com/macros/s/AKfycbxNu27V2Y2LuKUIQMK8lX1y0joB6YmG6hUwB1fNeVbgzEh22TcDGrOak03Fk3uBHmz-/exec'
#     payload = {
#         "route": "device-detail",
#         "key": key
#     }
#     response = requests.post(r, json=payload)

#     if response.status_code == 200:
#         data = response.json()
#         data = data.get("data", [])

#         # Main device DataFrame
#         device_info = {
#             'key': data['key'],
#             'device_name': data['device_name'],
#             'device_image': data['device_image'],
#             'display_size': data['display_size'],
#             'display_res': data['display_res'],
#             'camera': data['camera'],
#             'video': data['video'],
#             'ram': data['ram'],
#             'chipset': data['chipset'],
#             'battery': data['battery'],
#             'batteryType': data['batteryType'],
#             'release_date': data['release_date'],
#             'body': data['body'],
#             'os_type': data['os_type'],
#             'storage': data['storage']
#         }

#         device_df = pd.DataFrame([device_info])
#         print(device_df.head())

#         # # Extract prices
#         # prices = []
#         # for storage, shops in data['prices'].items():
#         #     for shop in shops:
#         #         prices.append({
#         #             'device_key': data['key'],
#         #             'storage': storage,
#         #             'shop_image': shop['shop_image'],
#         #             'price': shop['price'],
#         #             'buy_url': shop['buy_url']
#         #         })
#         # prices_df = pd.DataFrame(prices)
#         # print(prices_df.head())

#         # Extract specifications
#         # TODO separate into multiple dataframes
#         specifications = []
#         for spec in data['more_specification']:
#             title = spec['title']
#             for item in spec['data']:
#                 specifications.append({
#                     'device_key': data['key'],
#                     'specification_title': title,
#                     'specification_detail': item['title'],
#                     'specification_data': ', '.join(item['data'])
#                 })
#         specifications_df = pd.DataFrame(specifications)
#         print(specifications_df)

#         # # Extract related devices
#         # related_devices = []
#         # for related in data['more_information']['Related Devices']:
#         #     related_devices.append({
#         #         'device_key': data['key'],
#         #         'related_device_name': related['device_name'],
#         #         'related_device_image': related['device_image'],
#         #         'related_device_key': related['key']
#         #     })
#         # related_devices_df = pd.DataFrame(related_devices)
#         # print(related_devices_df.head())

#         # Connect to PostgreSQL
#         # try:
#         #     engine = create_engine('postgresql://username:password@localhost:5432/mydatabase')
#         #     # Load DataFrames into PostgreSQL
#         #     device_df.to_sql('devices', engine, if_exists='append', index=False)
#         #     prices_df.to_sql('device_prices', engine, if_exists='append', index=False)
#         #     specifications_df.to_sql('device_specifications', engine, if_exists='append', index=False)
#         #     related_devices_df.to_sql('related_devices', engine, if_exists='append', index=False)

#         #     print("Data loaded successfully!")
        
#         # except Exception as e:
#         #     print("Database error:", e)

#     else:
#         print("Error:", response.status_code, response.text)

#     return None

# def load_brands_to_db():
#     pass

# def load_devices_to_db():
#     pass


