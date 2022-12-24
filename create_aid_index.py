import sqlite3

conn = sqlite3.connect('/home/ec2-user/press_release_data/press_release_headlines.db', sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
c = conn.cursor()

c.execute("CREATE INDEX headline_1_index ON headline_data (headline_1);")
