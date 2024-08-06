import requests
import json
import pandas as pd
from sqlalchemy import create_engine

def get_brands():
    r = requests.get("https://script.google.com/macros/s/AKfycbxNu27V2Y2LuKUIQMK8lX1y0joB6YmG6hUwB1fNeVbgzEh22TcDGrOak03Fk3uBHmz-/exec?route=brand-list")
    
    if r.status_code == 200:
        data = r.json()
        brands = pd.DataFrame(data.get('data', []))

        return brands
    else:
        print(f"Failed to retrieve data. Status code: {r.status_code}")

        return pd.DataFrame()  

def get_devices():
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


def get_device_details(key):
    # TODO Divide these to multiple functions
    r = 'https://script.google.com/macros/s/AKfycbxNu27V2Y2LuKUIQMK8lX1y0joB6YmG6hUwB1fNeVbgzEh22TcDGrOak03Fk3uBHmz-/exec'
    payload = {
        "route": "device-detail",
        "key": key
    }
    response = requests.post(r, json=payload)

    if response.status_code == 200:
        data = response.json()
        data = data.get("data", [])

        # Main device DataFrame
        device_info = {
            'key': data['key'],
            'device_name': data['device_name'],
            'device_image': data['device_image'],
            'display_size': data['display_size'],
            'display_res': data['display_res'],
            'camera': data['camera'],
            'video': data['video'],
            'ram': data['ram'],
            'chipset': data['chipset'],
            'battery': data['battery'],
            'batteryType': data['batteryType'],
            'release_date': data['release_date'],
            'body': data['body'],
            'os_type': data['os_type'],
            'storage': data['storage']
        }

        device_df = pd.DataFrame([device_info])
        print(device_df.head())

        # Extract prices
        prices = []
        for storage, shops in data['prices'].items():
            for shop in shops:
                prices.append({
                    'device_key': data['key'],
                    'storage': storage,
                    'shop_image': shop['shop_image'],
                    'price': shop['price'],
                    'buy_url': shop['buy_url']
                })
        prices_df = pd.DataFrame(prices)
        print(prices_df.head())

        # Extract specifications
        # TODO separate into multiple dataframes
        specifications = []
        for spec in data['more_specification']:
            title = spec['title']
            for item in spec['data']:
                specifications.append({
                    'device_key': data['key'],
                    'specification_title': title,
                    'specification_detail': item['title'],
                    'specification_data': ', '.join(item['data'])
                })
        specifications_df = pd.DataFrame(specifications)
        print(specifications_df)

        # # Extract related devices
        # related_devices = []
        # for related in data['more_information']['Related Devices']:
        #     related_devices.append({
        #         'device_key': data['key'],
        #         'related_device_name': related['device_name'],
        #         'related_device_image': related['device_image'],
        #         'related_device_key': related['key']
        #     })
        # related_devices_df = pd.DataFrame(related_devices)
        # print(related_devices_df.head())

        # Connect to PostgreSQL
        # try:
        #     engine = create_engine('postgresql://username:password@localhost:5432/mydatabase')
        #     # Load DataFrames into PostgreSQL
        #     device_df.to_sql('devices', engine, if_exists='append', index=False)
        #     prices_df.to_sql('device_prices', engine, if_exists='append', index=False)
        #     specifications_df.to_sql('device_specifications', engine, if_exists='append', index=False)
        #     related_devices_df.to_sql('related_devices', engine, if_exists='append', index=False)

        #     print("Data loaded successfully!")
        
        # except Exception as e:
        #     print("Database error:", e)

    else:
        print("Error:", response.status_code, response.text)

    return None

def load_brands_to_db():
    pass

def load_devices_to_db():
    pass


print(get_brands().head(10))
print(get_devices().head(10))


# get_device_details("apple_iphone_13_pro_max-11089")
