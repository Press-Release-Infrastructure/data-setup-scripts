import gzip
import regex as re
import os
import sys
from datetime import datetime
import sqlite3
import time

filename = sys.argv[1]
file_num = int(sys.argv[2])

# try:
#     with open('./python_time_so_far.txt', 'r') as python_time_so_far:
#         python_time = float(python_time_so_far.readlines()[0])
# except:
#     python_time = 0.0

# try:
#     with open('./sql_time_so_far.txt', 'r') as sql_time_so_far:
#         sql_time = float(sql_time_so_far.readlines()[0])
# except:
#     sql_time = 0.0

# time_incr = 1000

def chunk_slice(chunk, tag_start, tag_end):
    start_ind = chunk.find(tag_start)
    end_ind = chunk.find(tag_end)
    if start_ind == -1 or end_ind == -1:
        return ''
    target = chunk[start_ind + len(tag_start):end_ind]
    return target.replace("\"", "")

conn = sqlite3.connect('/home/ec2-user/data_setup_scripts/press_release_headlines.db', sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
c = conn.cursor()

# precompile regex
aid_regex = re.compile('<dc:identifier identifierScheme="PGUID">urn:contentItem:(.*)<\/dc:identifier>')
lang_regex = re.compile('<classification classificationScheme="language">[\s|\S]*?<className>([a-zA-Z]*)<\/className>[\s|\S]*?<\/classification>')
indexing_chunk_regex = re.compile('<classificationGroup classificationScheme="indexing-terms">([\s|\S]*?)<\/classificationGroup>')
indexing_term_regex = re.compile('<classificationItem score="([0-9]*)">[\s|\S]*?<className>(.*?)<\/className>[\s|\S]*?<\/classificationItem>')
company_chunk_regex = re.compile('<classification classificationScheme="company">([\s|\S]*?)</classification>')
company_term_regex = re.compile('<classificationItem score="([0-9]*)">[\s|\S]*?<className>(.*?)<\/className>[\s|\S]*?<\/classificationItem>')
ticker_chunk_regex = re.compile('<classification classificationScheme="ticker">([\s|\S]*?)</classification>')
ticker_term_regex = re.compile('<classificationItem score="([0-9]*)">[\s|\S]*?<className>(.*?)<\/className>[\s|\S]*?<\/classificationItem>')

python_time = 0
sql_time = 0

with gzip.open(filename, 'rt') as headline_dump:
    headline_text = headline_dump.read()
    headline_chunks = re.findall('<articleDoc xmlns:xsi[\S|\s]*?<\/articleDoc>', headline_text)
    for chunk in headline_chunks:
        python_start = time.time()
        try:
            h1 = chunk_slice(chunk, '<nitf:hl1>', '</nitf:hl1>')
        except:
            h1 = ''

        try:
            h2 = chunk_slice(chunk, '<nitf:hl2>', '</nitf:hl2>')
        except:
            h2 = ''

        try:
            aid = re.search('<dc:identifier identifierScheme="PGUID">urn:contentItem:(.*)<\/dc:identifier>', chunk).group(1)
        except:
            aid = ''

        try:
            curr_pub_date = datetime.striptime(' '.join(chunk_slice(chunk, '<dateText>', '</dateText>').split(' ')[:-1]), '%B %d, %Y')
        except:
            curr_pub_date = ''

        try:
            nw = chunk_slice(chunk, '<publicationName>', '</publicationName>')
        except:
            nw = ''

        try:
            lang = re.search(lang_regex, chunk).group(1) 
        except:
            lang = ''

        try:
            indexing_chunk = re.search(indexing_chunk_regex, chunk).group(1)
            curr_indexing_terms = re.findall(indexing_term_regex, indexing_chunk)
        except:
            curr_indexing_terms = []

        try:
            company_chunk = str(re.search(company_chunk_regex, chunk))
            curr_company_terms = re.findall(company_term_regex, company_chunk)
        except:
            curr_company_terms = []

        try:
            ticker_chunk = str(re.search(ticker_chunk_regex, chunk))
            curr_ticker_terms = re.findall(ticker_term_regex, ticker_chunk)
        except:
            curr_ticker_terms = []

        curr_it = ['{}:{}'.format(i[0], i[1]).replace("\"", "").replace("'", "") for i in curr_indexing_terms]
        curr_ct = ['{}:{}'.format(i[0], i[1]).replace("\"", "").replace("'", "") for i in curr_company_terms]
        curr_tt = ['{}:{}'.format(i[0], i[1]).replace("\"", "").replace("'", "") for i in curr_ticker_terms]
        
        python_diff = time.time() - python_start 
        python_time += python_diff 
        sql_start = time.time()

        c.execute('''
            INSERT INTO headline_data(headline_1, headline_2, article_id, newswire, publication_date, language, indexing_terms, company_terms, ticker_terms, source_file)
            VALUES
                ("{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}")
        '''.format(h1, h2, aid, nw, curr_pub_date, lang, curr_it, curr_ct, curr_tt, filename))

        sql_time += time.time() - sql_start

conn.commit()

# with open('./python_time_so_far.txt', 'w') as python_time_so_far:
#     python_time_so_far.write(str(python_time))

# with open('./sql_time_so_far.txt', 'w') as sql_time_so_far:
#     sql_time_so_far.write(str(sql_time))

if file_num % 100 == 0:
    file_size = os.path.getsize(filename)
    print('Num: {} Python Time: {} SQL Time: {} File size: {}'.format(file_num, python_time, sql_time, file_size))

# if file_num % time_incr == 0:
#     with open('./python_time_overall.txt', 'a') as python_time_overall:
#         python_time_overall.write('{} {}'.format(file_num, python_time))

#     with open('./sql_time_overall.txt', 'a') as sql_time_overall:
#         sql_time_overall.write('{} {}'.format(file_num, sql_time))
    
#     python_time = 0
#     sql_time = 0
