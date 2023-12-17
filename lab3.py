import sqlite3
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import duckdb
from time import perf_counter
from statistics import median

# Postgres import

# engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')
# df = pd.read_csv("nyc_yellow_tiny.csv")
# df.to_sql('trips', engine, if_exists='replace', index=False)


# Postgres queries

# db_params = {
#     'dbname': 'postgres',
#     'user': 'postgres',
#     'password': 'postgres',
#     'host': 'localhost',
#     'port': '5432'
# }
# connection = psycopg2.connect(**db_params)
# cursor = connection.cursor()
# arr = []
# for i in range(20):
#     start = perf_counter()
#     # cursor.execute('SELECT "VendorID", count(*) FROM trips GROUP BY 1;')
#     # cursor.execute('SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1;')
#     # cursor.execute('SELECT passenger_count, extract(year from tpep_pickup_datetime::date), count(*) FROM trips GROUP BY 1, 2;')
#     cursor.execute('SELECT passenger_count, extract(year from tpep_pickup_datetime::date), round(trip_distance), count(*) FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;')
#     end = perf_counter()
#     arr.append(end - start)
# print("Postgres, time:", median(arr))
# cursor.close()
# connection.close()


# sqlite import

# with sqlite3.connect("tables/nyc_table.db") as conn:
#     cursor = conn.cursor()
#     tiny = "nyc_yellow_tiny.csv"
#     df = pd.read_csv(tiny)
#     df.to_sql('trips', conn, if_exists='replace', index=False)
#     conn.commit()


# sqlite queries

# with sqlite3.connect("tables/nyc_table1.db") as conn:
#     cursor = conn.cursor()
#     arr = []
#     for i in range(20):
#         start = perf_counter()
#         # cursor.execute('SELECT "VendorID", count(*) FROM trips GROUP BY 1;')
#         # cursor.execute('SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1;')
#         # cursor.execute('''SELECT passenger_count, strftime('%Y', tpep_pickup_datetime) AS "year", count(*) FROM trips GROUP BY 1, 2;''')
#         cursor.execute('''SELECT passenger_count, strftime('%Y', tpep_pickup_datetime) AS "year", round(trip_distance), count(*) FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;''')
#         end = perf_counter()
#         arr.append(end - start)
#     print("Sqlite, time:", median(arr))
#     conn.commit()


# duckdb queries

# conn = duckdb.connect()
# conn.execute('CREATE TABLE trips AS FROM "nyc_yellow_tiny.csv";')
# arr = []
# for i in range(20):
#     start = perf_counter()
#     # conn.execute('SELECT "VendorID", count(*) FROM trips GROUP BY 1;')
#     # conn.execute('SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1;')
#     # conn.execute('SELECT passenger_count, extract(year from tpep_pickup_datetime), count(*) FROM trips GROUP BY 1, 2;')
#     conn.execute('SELECT passenger_count, extract(year from tpep_pickup_datetime::date), round(trip_distance), count(*) FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;')
#     end = perf_counter()
#     arr.append(end - start)
# print("Duckdb, time:", median(arr))
# conn.close()


# pandas queries

engine = create_engine('sqlite:///tables/nyc_table2.db')
df = pd.read_csv("nyc_yellow_tiny.csv")
df.to_sql('trips', engine, if_exists='replace', index=False)
arr = []
for i in range(20):
    start = perf_counter()
    # query = 'SELECT "VendorID", count(*) FROM trips GROUP BY 1;'
    # query = 'SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1;'
    # query = '''SELECT passenger_count, strftime('%Y', tpep_pickup_datetime) AS "year", count(*) FROM trips GROUP BY 1, 2;'''
    query = '''SELECT passenger_count, strftime('%Y', tpep_pickup_datetime) AS "year", round(trip_distance), count(*) FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;'''
    pd.read_sql(query, con=engine)
    end = perf_counter()
    arr.append(end - start)
print("Pandas, time:", median(arr))

