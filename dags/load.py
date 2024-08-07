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
                    device_id INT UNIQUE,
                    device_name VARCHAR,
                    device_type VARCHAR,
                    device_image VARCHAR,
                    key VARCHAR,
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



def load_devices_to_db(devices_df):
    pass