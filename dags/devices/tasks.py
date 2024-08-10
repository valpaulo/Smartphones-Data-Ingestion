from airflow.decorators import dag, task_group, task
from datetime import datetime, timedelta
import pandas as pd
# import json
from devices.extract import get_brands, get_devices, get_device_keys, get_all_devices_info
from devices.load import init_brands_table, init_devices_table, load_brands_to_db, load_devices_to_db, load_specs_to_db, init_specs_table


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

@dag(
    'get_specs',
    description='Get all device specifications',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args
)
def get_specs():
    @task
    def get_device_keys_task():
        device_keys_df = get_device_keys()
        return device_keys_df

    @task
    def get_all_devices_info_task(device_keys_df):
        specs_df = get_all_devices_info(device_keys_df)
        return specs_df

    @task
    def init_specs_table_task():
        init_specs_table()

    @task
    def load_specs_to_db_task(specs_df):
        load_specs_to_db(specs_df)

    # Task dependencies
    device_keys_df = get_device_keys_task()
    specs_df = get_all_devices_info_task(device_keys_df)
    init = init_specs_table_task()
    load = load_specs_to_db_task(specs_df)
    
    init >> load

get_specs()


