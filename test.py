list_of_data = new_df.merge(
            old_df.rename(columns={'value': 'old_value'}), on=pk, how='left',
        )

list_of_data['has_match'] = ~list_of_data.old_value.isna()
list_of_data['different_value'] =  list_of_data.value == list_of_data.old_value
list_of_data['strategy'] = np.where(x.has_match, np.where(x.different_value, 'update', 'skip'), 'insert')


def get_strategy(x):
    if x.has_match:
        if x.different_value:
            return 'update'
        else:
            return 'skip'
    else:
        return 'insert'

list_of_data['strategy'] = list_of_data.apply(get_strategy, axis=1)



"""
.assign(
            has_match=lambda x: ~x.old_value.isna(),
            different_value=lambda x: x.value == x.old_value,
            strategy=lambda x: np.where(x.has_match, np.where(x.different_value, 'update', 'skip'), 'insert')
        )
"""

conn = sqlite3.connect("database.db")
cursor = conn.cursor()

for _, row in df.iterrows():
    cursor.execute("INSERT INTO users (id, name, date) VALUES (?, ?, ?)", (row["id"], row["name"], row["date"].strftime('%Y-%m-%d')))

conn.commit()
conn.close()
print("Data inserted successfully!")

update_df = list_of_data[list_of_data["strategy"] == 'update']
update_df = update_df[['user_x', 'status_x', 'lat', 'lon', 'date', 'value', 'insert_date_x']]
update_df = update_df.rename(
    columns={"user_x": "user", "status_x": "status", "insert_date_x": "insert_date"})

print(update_df)
column_names = update_df.columns.tolist()
print(column_names)

print("update_df")
print(update_df)
update_df = list_of_data[list_of_data["strategy"] == 'update']
update_df = update_df[['user_x', 'status_x', 'lat', 'lon', 'date', 'value', 'insert_date_x']]
update_df = update_df.rename(
    columns={"user_x": "user", "status_x": "status", "insert_date_x": "insert_date"})

column_names = update_df.columns.tolist()
print(column_names)

conn = sqlite3.connect("test.db")  # Use MySQL/PostgreSQL connection if needed
cursor = conn.cursor()
for _, row in update_df.iterrows():
    cursor.execute("""
                UPDATE weather_new
                SET user = ?, status = ?, value = ?, insert_date = ?
                WHERE date = ? and lat=? and lon=?
            """, (
        row["user_x"], row["status_x"], row["value"], row["insert_date_x"], row["date"], row["lat"],
        row["lon"]))

conn.commit()
conn.close()