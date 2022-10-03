import psycopg2
import csv
from dateutil.parser import parse


conn = psycopg2.connect("host=localhost dbname=run_markers user=postgres password=password")
cur = conn.cursor()

cur.execute("DROP TABLE IF EXISTS table2")
cur.execute("""
    CREATE TABLE IF NOT EXISTS table2 (
        id SERIAL PRIMARY KEY,
        CAMPAIGN varchar(100),
        Date DATE,
        MA_ID varchar(100),
        PROPOSED integer,
        NOT_ELIGIBLE integer,
        IN_CONTROL_GROUP integer,
        CAPPED integer,
        ACCEPTED integer,
        FULFILLED integer
    )
""")
with open('test_file_2.csv', 'r') as f:
    csvReader = csv.reader(f)
    for row in csvReader:
        if len(row) == 9:
            row[1] = parse(row[1])
            cur.execute("INSERT INTO table2(CAMPAIGN,DATE,MA_ID,PROPOSED,NOT_ELIGIBLE,IN_CONTROL_GROUP,CAPPED,ACCEPTED,FULFILLED) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", row)
conn.commit()
