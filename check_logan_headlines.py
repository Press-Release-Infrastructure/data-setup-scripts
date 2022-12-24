import sqlite3
import pandas as pd

conn = sqlite3.connect('/home/ec2-user/press_release_data/press_release_headlines.db', sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
c = conn.cursor()

not_found = []
count = 0

with open('headlines_no_duplicates.txt') as f:
    headlines = [i.split(' || ') for i in f.read().splitlines() if i.strip() != '']
    for h, _, _ in headlines:
        # cmd = 'SELECT * from headline_data where headline_1 like "{}%"'.format(h.split(' ')[0])
        count += 1
        try:
            cmd = 'SELECT * from headline_data where headline_1 = "{}"'.format(h)
            c.execute(cmd)
            result = c.fetchall()
            if not len(result):
                not_found.append(h)
                print(h, 'NO', len(not_found))
            else:
                print(h, 'YES', count - len(not_found))
        except:
            not_found.append(h)
            print(h, 'ERROR', len(not_found))

print('num found', count - len(not_found))
print('not found', len(not_found))

df = pd.DataFrame({'not_found': not_found})

df.to_csv('logan_not_exact_match.csv')
