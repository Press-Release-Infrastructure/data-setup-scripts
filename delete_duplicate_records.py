import sqlite3

conn = sqlite3.connect('/home/ec2-user/data_setup_scripts/press_release_headlines.db')
c = conn.cursor()

c.execute('''
       DELETE from headline_data
        where headline_id not in
         (
         select  min(headline_id)
         from headline_data
         group by headline_1, headline_2, article_id, newswire, publication_date, language
         ) 
        ''')

conn.commit()
