# identify the most common tags in db (after populating with randomized subset of data files)

import sqlite3
import pandas as pd

conn = sqlite3.connect('/home/ec2-user/data-setup-scripts/press_release_headlines.db', detect_types = sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
c = conn.cursor()

c.execute("SELECT indexing_terms FROM headline_data")
rows = c.fetchall()

tag_dict = {}
for r in rows:
    tags_to_incr = [i[3:].lower().strip() for i in eval(r[0])]
    for t in tags_to_incr:
        if t in tag_dict: tag_dict[t] += 1
        else: tag_dict[t] = 1

# print the 500 most common tags + their counts
most_common = sorted(tag_dict.items(), key = lambda item: item[1], reverse = True)[:500]
common_tags_df = pd.DataFrame({"tag_name": [i[0] for i in most_common], "frequency": [i[1] for i in most_common]})
common_tags_df.to_csv('most_common_tags.csv')
