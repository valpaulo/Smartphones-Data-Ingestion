from airflow.decorators import task
import requests
import pandas as pd
import pyarrow as pa

@task
def get_brands(**kwargs):
    r = requests.get("https://script.google.com/macros/s/AKfycbxNu27V2Y2LuKUIQMK8lX1y0joB6YmG6hUwB1fNeVbgzEh22TcDGrOak03Fk3uBHmz-/exec?route=brand-list")
    
    if r.status_code == 200:
        data = r.json()
        brands = pd.DataFrame(data.get('data', []))

        return brands
    else:
        print(f"Failed to retrieve data. Status code: {r.status_code}")

        return pd.DataFrame()  

@task
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
