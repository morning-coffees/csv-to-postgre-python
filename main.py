import psycopg2
import csv
from datetime import datetime
from dateutil.parser import parse


conn = psycopg2.connect("host=localhost dbname=run_markers user=postgres password=password")
cur = conn.cursor()

cur.execute("DROP TABLE IF EXISTS table1")

cur.execute("""
    CREATE TABLE IF NOT EXISTS table1 (
        id SERIAL PRIMARY KEY,
        CAMPAIGN varchar(100),
        LAST_UPDATE timestamp,
        CAMPAIGN_ID varchar(5),
        CAMPAIGN_NAME varchar(100),
        CAMPAIGN_SEGMENT_NAME varchar(100),
        ROW_COUNT integer
    )
""")
with open('test_file.csv', 'r') as f:
    csvReader = csv.reader(f)
    for row in csvReader:
        if len(row) == 6:
            # last_update = datetime.strptime(row[1], '%m/%d/%Y %H:%M:%S')
            row[1] = parse(row[1])
            row[5] = row[5].replace(",", "")
            cur.execute("INSERT INTO table1(CAMPAIGN,LAST_UPDATE,CAMPAIGN_ID,CAMPAIGN_NAME,CAMPAIGN_SEGMENT_NAME,ROW_COUNT) VALUES (%s, %s, %s, %s, %s, %s)", row)
conn.commit()
