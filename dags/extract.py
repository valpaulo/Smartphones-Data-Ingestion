import requests
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook



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

    raw_sql = """ SELECT key FROM devices """

    device_keys_df = pd.read_sql(raw_sql, engine)

    return device_keys_df
     
def get_device_info(key):
    r = 'https://script.google.com/macros/s/AKfycbxNu27V2Y2LuKUIQMK8lX1y0joB6YmG6hUwB1fNeVbgzEh22TcDGrOak03Fk3uBHmz-/exec'
    payload = {
        "route": "device-detail",
        "key": key
    }
    response = requests.post(r, json=payload)

    if response.status_code == 200:
        data = response.json().get("data", {})

        if data:  # If data is not empty
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
    print(f"Error fetching data for key: {key}, status: {response.status_code}")
    return pd.DataFrame() 

def get_all_devices_info(device_keys_df):
    all_device_info_dfs = []

    for key in device_keys_df['key']:
        device_info_df = get_device_info(key)
        if not device_info_df.empty:
            all_device_info_dfs.append(device_info_df)

    all_devices_info_df = pd.concat(all_device_info_dfs, ignore_index=True)
    print(all_devices_info_df.head())

    return all_devices_info_df
