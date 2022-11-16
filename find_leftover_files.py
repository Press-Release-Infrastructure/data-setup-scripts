import sqlite3
import pandas as pd
import os

with open('./ls_output.txt') as f:
    total_files = set(f.read().splitlines())

conn = sqlite3.connect('/home/ec2-user/data_setup_scripts/press_release_headlines.db')
c = conn.cursor()

c.execute('''
        SELECT DISTINCT(source_file) FROM headline_data;
        ''')

files_done = set([i[0].split('/')[-1] for i in c.fetchall()])
files_todo = total_files - files_done

files_todo_df = pd.DataFrame({'filename': list(files_todo)})
files_todo_df.to_csv('files_todo.csv', index = False, header = None)
