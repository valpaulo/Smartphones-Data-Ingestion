from airflow.providers.postgres.hooks.postgres import PostgresHook
from pandas import DataFrame


def init_brands_table():
    pg_hook = PostgresHook(postgres_conn_id = "postgres_conn_id")
    raw_sql = """ CREATE TABLE IF NOT EXISTS brands(
                    brand_id INT,
                    brand_name VARCHAR,
                    key VARCHAR PRIMARY KEY UNIQUE
                    );"""
    pg_hook.run(raw_sql)



def init_devices_table():
    pg_hook = PostgresHook(postgres_conn_id = "postgres_conn_id")
    raw_sql = """ CREATE TABLE IF NOT EXISTS devices(
                    device_id INT,
                    device_name VARCHAR,
                    device_type VARCHAR,
                    key VARCHAR UNIQUE,
                    brand_id INT,
                    brand_name VARCHAR,
                    brand_key VARCHAR
                    );"""
    pg_hook.run(raw_sql)



def load_brands_to_db(brands_df: DataFrame):
    pg_hook = PostgresHook(postgres_conn_id = "postgres_conn_id")

    values_str = ",\n".join(brands_df.apply(format_brands_row, axis=1))

    insert_sql = f""" INSERT INTO brands (brand_id, brand_name, key)
                        VALUES {values_str}
                        ON CONFLICT (key) DO NOTHING;
                        """
    pg_hook.run(insert_sql)


def format_brands_row(row):
    return f"""({row['brand_id']},
                '{row['brand_name']}',
                '{row['key']}')
            """



def load_devices_to_db(devices_df: DataFrame):
    pg_hook = PostgresHook(postgres_conn_id = "postgres_conn_id")

    values_str = ",\n".join(devices_df.apply(format_devices_row, axis=1))

    insert_sql = f""" INSERT INTO devices (
                            device_id, 
                            device_name, 
                            device_type,
                            key,
                            brand_id,
                            brand_name,
                            brand_key)
                        VALUES {values_str}
                        ON CONFLICT (key) DO NOTHING;
                        """
    pg_hook.run(insert_sql)

def format_devices_row(row):
    device_name = row['device_name'].replace("'", "''")
    key = row['key'].replace("'","''")
    return f"""({row['device_id']},
                '{device_name}',
                '{row['device_type']}',
                '{key}',
                {row['brand_id']},
                '{row['brand_name']}',
                '{row['brand_key']}')"""