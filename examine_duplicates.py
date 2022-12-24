import sqlite3
import numpy as np
import random 
import pandas as pd

np.random.seed(0)
random.seed(0)

conn = sqlite3.connect('/home/ec2-user/data_setup_scripts/press_release_headlines.db', sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
c = conn.cursor()

headline = []
article_id = []
match = []
num_match = []

with open('headlines_no_duplicates.txt', 'r') as f:
    lines = [i for i in f.read().splitlines() if i.strip() != '']
    for l in lines:
        split_l = l.split(' || ')
        h, _, aid = split_l
        c.execute("SELECT * FROM headline_data where article_id=\"{}\"".format(aid))
        row_match = c.fetchall()
        
        headline.append(h)
        article_id.append(aid)
        num_match.append(len(list(row_match)))
        match.append(str(row_match))

dup_df = pd.DataFrame({'headline': headline, 'article_id': article_id, 'num_match': num_match, 'match': match})
dup_df.to_csv('duplicates.csv')

print(dup_df['num_match'].value_counts())

# c.execute("SELECT headline_id FROM headline_data order by headline_id desc limit 1")
# num_rows = c.fetchall()[0][0]
# print(num_rows)

# headline_id = []
# headline_1 = []
# headline_2 = []
# article_id = []
# newswire = []
# publication_date = []
# source_file = []

# idx_subset = np.random.choice(num_rows, 1000)
# for i in idx_subset:
#     c.execute("SELECT * FROM headline_data where headline_id={}".format(i + 1))
#     row_match = c.fetchall()[0]
#     h_id, h1, h2, aid, nw, pub, _, _, _, _, sf = row_match 
#     headline_id.append(h_id)
#     headline_1.append(h1)
#     headline_2.append(h2)
#     article_id.append(aid)
#     newswire.append(nw)
#     publication_date.append(pub)
#     source_file.append(sf)

# df_sample = pd.DataFrame({
#     'headline_id': headline_id,
#     'headline_1': headline_1,
#     'headline_2': headline_2,
#     'article_id': article_id,
#     'newswire': newswire,
#     'publication_date': publication_date,
#     'source_file': source_file
# })

# df_sample.to_csv('df_sample.csv')

# # find places where article id is the same
# groups = df_sample.groupby('article_id').size()
# print(groups.loc[groups == 2])

# dups_ex = df_sample.loc[df_sample.article_id.str.strip() == '5JY6-J3J1-JCNX-304J-00000-00']
# print('Duplicate 1')
# print(list(dups_ex.iloc[0]))
# print('Duplicate 2')
# print(list(dups_ex.iloc[1]))
# print()

# empty_aid_ex = df_sample.loc[df_sample.article_id.str.strip() == '']
# print('Empty Article ID')
# print(list(empty_aid_ex.iloc[0]))