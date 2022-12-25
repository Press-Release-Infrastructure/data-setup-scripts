import pandas as pd 
import sys
from datetime import datetime, timedelta
import sqlite3
from fuzzywuzzy import fuzz

conn = sqlite3.connect('/home/ec2-user/press_release_data/press_release_headlines_large.db', sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
# conn = sqlite3.connect('/home/ec2-user/press_release_data/press_release_headlines.db', sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
c = conn.cursor()

cb_acq = pd.read_csv('acq_cb.csv')
non_cb_acq = pd.read_csv('non_acq_cb.csv')

def check_word(w, target):
    return fuzz.ratio(target, w) > 90

def check_headline(h, target):
    for w in h.split(' '):
        if check_word(w.lower().strip(), target):
            return True
    return False

def find_possible_acqs(row):
    acquirer, acquired, acq_date = row['acquirer'], row['acquired'], row['acq_date']
    acquirer = acquirer.lower().strip() if type(acquirer) == str else ''
    acquired = acquired.lower().strip() if type(acquired) == str else ''
    # narrow rows by acq date
    if type(acq_date) != str: return str([])
    acq_date = datetime.strptime(acq_date, '%Y-%m-%d')
    range_start = (acq_date - timedelta(days = 30)).date()
    range_end = (acq_date + timedelta(days = 30)).date()
    cmd = 'select headline_1, headline_2, publication_date from headline_data where publication_date between "{}" and "{}";'.format(range_start, range_end)
    c.execute(cmd)
    result = [i[0] + ' ' + i[1] for i in c.fetchall()]
    candidate_matches = []
    for r in result:
        if check_headline(r, acquirer) and check_headline(r, acquired):
            candidate_matches.append(r)
    print(acquirer, acquired, candidate_matches)
    return str(candidate_matches)

cb_acq['headline_matches'] = cb_acq.apply(find_possible_acqs, axis = 1)
cb_acq.to_csv('cb_acq_modified.csv')
