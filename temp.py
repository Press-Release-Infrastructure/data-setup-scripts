import numpy as np 
import sqlite3
import pandas as pd

conn = sqlite3.connect('/home/ec2-user/data_setup_scripts/press_release_headlines.db', sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
c = conn.cursor()

start = 26747300
end = 27574166
for i in range(start, end + 1):
    c.execute("SELECT source_file FROM headline_data where headline_id = {}".format(i))
    result = c.fetchall()[0][0]
    filename = result.split('/')[-1]
    cmd = "update headline_data set source_file_clean = \"{}\" where headline_id = {}".format(filename, i)
    c.execute(cmd)

    if i % 100 == 0 or i == end:
        print("commiting up to row {}".format(i))
        conn.commit()