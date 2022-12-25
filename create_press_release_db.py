import sqlite3

conn = sqlite3.connect('/home/ec2-user/data_setup_scripts/press_release_headlines.db')
c = conn.cursor()

c.execute('''
        CREATE TABLE IF NOT EXISTS headline_data
        (
            [headline_id] INTEGER PRIMARY KEY,
            [headline_1_hash] TEXT,
            [headline_1] TEXT,
            [headline_2] TEXT,
            [headline_2_hash] TEXT,
            [article_id] TEXT,
            [newswire] TEXT,
            [publication_date] TEXT,
            [publication_date_hash] TEXT,
            [language] TEXT,
            [indexing_terms] TEXT,
            [source_file] TEXT
        )
        ''')

c.execute('CREATE INDEX database_id ON headline_data (headline_1_hash, headline_2_hash, publication_date_hash)')

conn.commit()

