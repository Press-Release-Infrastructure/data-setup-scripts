import gzip
import regex as re
import os
import sys
from datetime import datetime
import sqlite3

file_path = sys.argv[1]

conn = sqlite3.connect('/home/ec2-user/data-setup-scripts/press_release_headlines.db', detect_types = sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
c = conn.cursor()

filename = os.path.basename(file_path)
with gzip.open(file_path, 'rt') as headline_dump:
    headline_text = headline_dump.read()
    headline_chunks = re.findall('<articleDoc xmlns:xsi[\S|\s]*?<\/articleDoc>', headline_text)
    for chunk in headline_chunks:
        try:
            h1 = re.search('<nitf:hedline>\s*<nitf:hl1>\s*(.*)\s*<\/nitf:hl1>', chunk).group(1).replace("\"", "")
        except:
            h1 = ''

        try:
            h2 = re.search('<nitf:hl2>\s*(.*)\s*<\/nitf:hl2>', chunk).group(1).replace("\"", "")
        except:
            h2 = ''

        try:
            aid = re.search('<dc:identifier identifierScheme="PGUID">urn:contentItem:(.*)<\/dc:identifier>', chunk).group(1)
        except:
            aid = ''

        try:
            curr_pub_date = datetime.strptime(' '.join(re.search('<dateText>(.*)<\/dateText>', chunk).group(1).split(' ')[:-1]), '%B %d, %Y')
        except:
            curr_pub_date = ''

        try:
            nw = re.search('<publicationName>(.*)</publicationName>', chunk).group(1) 
        except:
            nw = ''

        try:
            lang = re.search('<classification classificationScheme="language">[\s|\S]*?<className>([a-zA-Z]*)<\/className>[\s|\S]*?<\/classification>', chunk).group(1) 
        except:
            lang = ''

        try:
            indexing_chunk = re.search('<classificationGroup classificationScheme="indexing-terms">([\s|\S]*?)<\/classificationGroup>', chunk).group(1)
            curr_indexing_terms = re.findall('<classificationItem score="([0-9]*)">[\s|\S]*?<className>(.*?)<\/className>[\s|\S]*?<\/classificationItem>', indexing_chunk)
        except:
            curr_indexing_terms = []

        try:
            company_chunk = str(re.search('<classification classificationScheme="company">([\s|\S]*?)</classification>', chunk))
            curr_company_terms = re.findall('<classificationItem score="([0-9]*)">[\s|\S]*?<className>(.*?)<\/className>[\s|\S]*?<\/classificationItem>', company_chunk)
        except:
            curr_company_terms = []

        try:
            ticker_chunk = str(re.search('<classification classificationScheme="ticker">([\s|\S]*?)</classification>', chunk))
            curr_ticker_terms = re.findall('<classificationItem score="([0-9]*)">[\s|\S]*?<className>(.*?)<\/className>[\s|\S]*?<\/classificationItem>', ticker_chunk)
        except:
            curr_ticker_terms = []

        curr_it = ['{}:{}'.format(i[0], i[1]).replace("\"", "").replace("'", "") for i in curr_indexing_terms]
        curr_ct = ['{}:{}'.format(i[0], i[1]).replace("\"", "").replace("'", "") for i in curr_company_terms]
        curr_tt = ['{}:{}'.format(i[0], i[1]).replace("\"", "").replace("'", "") for i in curr_ticker_terms]
        
        c.execute('''
            INSERT INTO headline_data(headline_1, headline_2, article_id, newswire, publication_date, language, indexing_terms, company_terms, ticker_terms, source_file)
            VALUES
                ("{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}")
        '''.format(h1, h2, aid, nw, curr_pub_date, lang, curr_it, curr_ct, curr_tt, filename))

conn.commit()
