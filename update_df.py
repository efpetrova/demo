import pandas as pd

df = pd.DataFrame(
    [{'a': 1, 'b': 'foobar'},
{'a': 2, 'b': 'foobar'}
     ]
)

commands = "update weather_new set b = '"+df['b'] + "' where id = " + df['a'].astype(str) + ";"
sql = "\n".join(commands)
print(sql)

If you have a Python string containing multiple SQL UPDATE queries separated by semicolons, you can execute them using sqlite3 like this:

import sqlite3

conn = sqlite3.connect("your_db.sqlite")
cursor = conn.cursor()

queries = """
update weather_new set b = 'foobar' where id = 1;
update weather_new set b = 'foobar' where id = 2;
"""

cursor.executescript(queries)
conn.commit()
conn.close()

Use .executescript() (not .execute() or .executemany()) when you have multiple SQL statements in one string.