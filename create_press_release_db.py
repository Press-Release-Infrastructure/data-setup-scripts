import sqlite3

conn = sqlite3.connect('/home/ec2-user/data_setup_scripts/press_release_headlines.db')
c = conn.cursor()

c.execute('''
        CREATE TABLE IF NOT EXISTS headline_data
        (
            [headline_id] INTEGER PRIMARY KEY,
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

conn.commit()

