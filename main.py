import psycopg2
import csv
conn = psycopg2.connect("host=localhost dbname=characterdb user=postgres password=password")
cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS characters (
        id SERIAL PRIMARY KEY,
        first_name varchar(100),
        last_name varchar(100),
        age integer
    )
""")

with open('test_file.csv', 'r') as f:
    csvReader = csv.reader(f)
    for row in csvReader:
        cur.execute("INSERT INTO characters(first_name,last_name,age) VALUES (%s, %s, %s)", row)
conn.commit()

cur.execute("SELECT SUM(age) FROM characters")
print (cur.fetchone())
