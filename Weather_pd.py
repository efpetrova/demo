import pandas as pd
from sqlalchemy import create_engine
import requests
from datetime import datetime, timezone
from datetime import timedelta

import requests
import sqlite3
from datetime import datetime, timezone
from datetime import timedelta


def get_data():
    now_utc = datetime.now(timezone.utc)
    end_date = now_utc + timedelta(days=3)
    username = 'home_petrova_ekaterina'
    password = '90sCg8VI8q'
    data = {}
    url = "https://api.meteomatics.com/"+str(now_utc.isoformat())+"--"+str(end_date.isoformat())+":PT1H/t_2m:C/41.382894,2.177432/json"
    response = requests.get(url, auth=(username, password))
    data = response.json()
    return data

def get_value_of_weather(p_dict_values):
    out = []
    for item in p_dict_values:
        for k in p_dict_values['data']:
           for i in k.get("coordinates"):
               for j in i.get("dates"):
                   record = {
                       'user': p_dict_values['user'],
                       'status': p_dict_values['status'],
                       'lat': p_dict_values.get('data', {})[0].get('coordinates')[0].get('lat'),
                       'lon': p_dict_values.get('data', {})[0].get('coordinates')[0].get('lon'),
                       'date': j.get('date'),
                       'value': j.get('value')
                   }
                   out.append(record)
    return out

data = get_data()

data = get_value_of_weather(data)
df = pd.DataFrame(data)
# Sample DataFrame
# Create SQLite engine
engine = create_engine("sqlite:///test.db")

# Store DataFrame into SQLite
df.to_sql("weather_pd", con=engine, if_exists="replace", index=False)

print("Data stored successfully!")
print(df)