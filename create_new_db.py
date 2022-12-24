import sqlite3
import zlib
import hashlib

old_conn = sqlite3.connect('/home/ec2-user/press_release_data/press_release_headlines.db', sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
new_conn = sqlite3.connect('/home/ec2-user/data_setup_scripts/press_release_headlines.db', sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)

old_c = old_conn.cursor()
new_c = new_conn.cursor()

new_c.execute('''
        CREATE TABLE IF NOT EXISTS headline_data
        (
            [database_id] TEXT PRIMARY KEY,
            [headline_1] TEXT,
            [headline_2] TEXT,
            [article_id] TEXT,
            [newswire] TEXT,
            [publication_date] TEXT,
            [language] TEXT,
            [indexing_terms] TEXT,
            [company_terms] TEXT,
            [ticker_terms] TEXT,
            [source_file] TEXT
        )
        ''')
# new_c.execute('''
#         CREATE INDEX headline_1_index ON headline_data (headline_1);
#         ''')

new_conn.commit()

start = 11957500
end = 27574166

for i in range(start, end + 1):
    try:
        old_c.execute('select * from headline_data where headline_id = {}'.format(i))
        result = old_c.fetchall()[0]
        _, headline_1, headline_2, article_id, newswire, pub_date, lang, it, ct, tt, _, _, source_file = result
        combined = '{}{}{}'.format(headline_1, headline_2, pub_date)
        database_id = hex(zlib.crc32(str.encode(combined)) & 0xffffffff)
        # database_id = hashlib.sha1(combined).hexdigest()
        new_c.execute('''
                    INSERT INTO headline_data(database_id, headline_1, headline_2, article_id, newswire, publication_date, language, indexing_terms, company_terms, ticker_terms, source_file)
                    VALUES
                        ("{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}")
                '''.format(database_id, headline_1, headline_2, article_id, newswire, pub_date, lang, it, ct, tt, source_file))
    except Exception as e:
        print('failed to complete row {} due to {}'.format(i, repr(e)))
        # print(database_id, headline_1)
        # a = new_c.execute('select * from headline_data where database_id = \"{}\"'.format(database_id))
        # print(a.fetchall())
        # print()

    if i % 100 == 0:
        print('committing up to {}'.format(i))
        new_conn.commit()