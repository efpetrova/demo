from datetime import datetime, timezone
from sqlalchemy import create_engine
from datetime import timedelta
import pandas as pd
import requests
import numpy as np
import time
import os

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def get_data():
    now_utc = datetime.now(timezone.utc)  # Already a datetime object
    end_date = now_utc + timedelta(days=3)
    print(end_date.strftime("%Y-%m-%dT%HZ"))  # Convert to desired format

    username = 'home_petrova_ekaterina'
    password = '90sCg8VI8q'
    url = "https://api.meteomatics.com/" + str(now_utc.strftime("%Y-%m-%dT%HZ")) + "--" + str(
        end_date.strftime("%Y-%m-%dT%HZ")) + ":PT1H/t_2m:C/41.382894,2.177432/json"
    print(url)
    response = requests.get(url, auth=(username, password))
    return response.json()


def get_value_of_weather(p_dict_values):
    out = []
    for k in p_dict_values['data']:
        for x in k.get("coordinates"):
            for item in x["dates"]:
                record = {
                    'user': p_dict_values['user'],
                    'status': p_dict_values['status'],
                    'lat': x['lat'],
                    'lon': x['lon'],
                    'date': item['date'],
                    'value': item['value'],
                    'insert_date': (datetime.now(timezone.utc)).strftime("%Y-%m-%d %H:%M:%S %Z%z")
                }
                out.append(record)
    print(len(out))
    return out


def daemon_task():
    engine = create_engine("sqlite:///test.db")
    print("path")
    print(os.path.abspath("test.db"))

    while True:
        try:
            print("Daemon running...")
            new_data = get_data()
            new_data = get_value_of_weather(new_data)

            if not new_data:
                print("No new data to process.")
                time.sleep(12 * 60 * 60)
                continue

            new_df = pd.DataFrame(new_data)
            print("new_df:")
            print(new_df.shape)

            with engine.connect() as conn:
                query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='weather_new'"
                exists = pd.read_sql_query(query, conn)

                if exists.empty:
                    print("Table does not exist")
                    new_df.to_sql("weather_new", con=conn, if_exists="append", index=False)
                    conn.commit()
                else:
                    old_df = pd.read_sql("SELECT * FROM weather_new", con=conn)

                    print("old_df:")
                    print(old_df.head())

                    # Define primary keys
                    pk = ["lat", "lon", "date"]

                    print(f"new_df.shape={new_df.shape}")
                    print(f"old_df.shape={old_df.shape}")

                    # Perform merge
                    list_of_data = new_df.merge(
                        old_df.rename(columns={'value': 'old_value'}), on=pk, how='left'
                    ).assign(
                        has_match=lambda x: ~x.old_value.isna(),
                        different_value=lambda x: x.value == x.old_value,
                        strategy=lambda x: np.where(x.has_match, np.where(x.different_value, 'update', 'skip'),
                                                    'insert')
                    )

                    print(f"list_of_data.shape={list_of_data.shape}")

                    # Extract records to insert
                    insert_df = list_of_data[list_of_data["strategy"] == 'insert']
                    insert_df = insert_df.rename(
                        columns={'user_x': 'user', 'status_x': 'status', 'insert_date_x': 'insert_date'})
                    insert_df = insert_df[['user', 'status', 'lat', 'lon', 'date', 'value', 'insert_date']]

                    if not insert_df.empty:
                        insert_df.to_sql("weather_new", con=engine, if_exists="append", index=False)
                        print("Data has inserted successfully!")

                    update_df = list_of_data[list_of_data["strategy"] == 'update']

                    print(f"insert_df.shape={insert_df.shape}")
                    print(f"update_df.shape={update_df.shape}")

            time.sleep(12 * 60 * 60)  # Sleep for 12 hours before the next run

        except Exception as e:
            print(f"Critical Error: {e}")
            time.sleep(60)  # Retry in 1 minute


def main():
    daemon_task()

if __name__ == '__main__':
    main()
