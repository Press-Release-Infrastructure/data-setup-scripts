import sqlite3
import hashlib

conn = sqlite3.connect('/home/ec2-user/data_setup_scripts/press_release_headlines.db', sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
c = conn.cursor()

start = 1
end = 27574166
for i in range(start, end + 1):
    try:
        c.execute("SELECT headline_1, headline_2, source_file FROM headline_data where headline_id = {}".format(i))
        result = ''.join(c.fetchall()[0])
        hash = hashlib.sha256(result.encode('utf-8')).hexdigest()
        cmd = "update headline_data set database_id = \"{}\" where headline_id = {}".format(str(hash), i)
        c.execute(cmd)
    except:
        continue

    if i % 100 == 0:
        print("commiting up to row {}".format(i))
        conn.commit()
