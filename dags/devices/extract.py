import requests
from requests.exceptions import RequestException
import time
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter


def get_brands(**kwargs):
    r = requests.get("https://script.google.com/macros/s/AKfycbxNu27V2Y2LuKUIQMK8lX1y0joB6YmG6hUwB1fNeVbgzEh22TcDGrOak03Fk3uBHmz-/exec?route=brand-list")
    
    if r.status_code == 200:
        data = r.json()
        brands_df = pd.DataFrame(data.get('data', []))

        return brands_df
    else:
        print(f"Failed to retrieve data. Status code: {r.status_code}")

        return pd.DataFrame()  


def get_devices(**kwargs):
    r = requests.get("https://script.google.com/macros/s/AKfycbxNu27V2Y2LuKUIQMK8lX1y0joB6YmG6hUwB1fNeVbgzEh22TcDGrOak03Fk3uBHmz-/exec?route=device-list")
    
    if r.status_code == 200:
        data = r.json()
        all_devices = []

        for brand in data.get("data", []): 
            device_list = brand.get("device_list", [])

            for device in device_list:
                device["brand_id"] = brand["brand_id"]
                device["brand_name"] = brand["brand_name"]
                device["brand_key"] = brand["key"]
                

            all_devices.extend(device_list)

        all_devices_df = pd.DataFrame(all_devices)

        return all_devices_df
    else:
        print(f"Failed to retrieve data. Status code: {r.status_code}")

        return pd.DataFrame()


def get_device_keys(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id = "postgres_conn_id")
    engine = pg_hook.get_sqlalchemy_engine()

    raw_sql = """ SELECT key FROM devices WHERE brand_name = 'Honor'; """ #TEMP BRAND Name

    device_keys_df = pd.read_sql(raw_sql, engine)

    return device_keys_df
     
def get_device_info(key, retries=10, delay=1):
    r = 'https://script.google.com/macros/s/AKfycbxNu27V2Y2LuKUIQMK8lX1y0joB6YmG6hUwB1fNeVbgzEh22TcDGrOak03Fk3uBHmz-/exec'
    payload = {
        "route": "device-detail",
        "key": key
    }

    for attempt in range(retries):
        try:
            response = requests.post(r, json=payload, timeout=20)
            if response.status_code == 200:
                data = response.json().get("data", {})
                if data:
                    device_info = {
                        'key': data.get('key'),
                        'device_name': data.get('device_name'),
                        'device_image': data.get('device_image'),
                        'display_size': data.get('display_size'),
                        'display_res': data.get('display_res'),
                        'camera': data.get('camera'),
                        'video': data.get('video'),
                        'ram': data.get('ram'),
                        'chipset': data.get('chipset'),
                        'battery': data.get('battery'),
                        'batteryType': data.get('batteryType'),
                        'release_date': data.get('release_date'),
                        'body': data.get('body'),
                        'os_type': data.get('os_type'),
                        'storage': data.get('storage')
                    }
                    return pd.DataFrame([device_info])
                elif response.status_code == 429:
                    print(f"Rate limit exceeded. Retrying in {delay} seconds...")
                    time.sleep(delay)
                    delay *= 2

            print(f"Attempt {attempt + 1} failed for key: {key}. Retrying in {delay} seconds...")
            time.sleep(delay)

        except RequestException as e:
            print(f"Request failed for device key: {key}. Error: {e}")
            time.sleep(delay)

    print(f"Failed to fetch data for device key: {key} after {retries} attempts.")
    return pd.DataFrame()


def get_all_devices_info(device_keys_df):
    all_device_info_df = []
    time_start = perf_counter()

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(get_device_info, key): key for key in device_keys_df['key']}

        for future in futures:
            try:
                device_info_df = future.result()
                all_device_info_df.append(device_info_df)
            except Exception as e:
                key = futures[future]
                print(f"Failed to get info for device key {key}: {e}")
                continue

    all_devices_info_df = pd.concat(all_device_info_df, ignore_index=True)
    df = all_devices_info_df
    df.to_csv("all_devices_info.csv", index=False)
    print(f"Time Elapsed for get_all_devices_info: {perf_counter() - time_start:.4f} s")
    
    return all_devices_info_df



