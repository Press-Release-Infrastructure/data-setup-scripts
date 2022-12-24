import sqlite3
import pandas as pd
import zlib

conn = sqlite3.connect('/home/ec2-user/press_release_data/press_release_headlines.db', sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
c = conn.cursor()

headlines_20k = pd.read_csv('headlines_20k.csv')

def process_headline(row):
    curr_h = row['headline']
    curr_dbid = row['database_id']
    c.execute('select * from headline_data where headline_1 = \"{}\" and database_id = \"{}\"'.format(curr_h, curr_dbid))
    results = c.fetchall()[0]
    headline_1 = results[1]
    headline_2 = results[2]
    pub_date = results[5]
    crc_hash = hex(zlib.crc32(str.encode('{}{}{}'.format(headline_1, headline_2, pub_date))) & 0xffffffff)
    return crc_hash

headlines_20k['crc_id'] = headlines_20k.apply(process_headline, axis = 1)
headlines_20k.to_csv('headlines_20k_crc.csv')
