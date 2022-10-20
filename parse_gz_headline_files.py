import gzip
import regex as re
import os
import sys
from datetime import datetime

file_path = sys.argv[1] #'9ec7c097-473e-41f3-b890-cdcd45d1d0ac.gz'
headline1 = []
headline2 = []
article_id = []
source_filename = []
pub_date = []
news_wire = []
language = []
indexing_terms = []
company_terms = []
ticker_terms = []

filename = os.path.basename(file_path)
with gzip.open(file_path, 'rt') as headline_dump:
    headline_text = headline_dump.read()
    headline_chunks = re.findall('<articleDoc xmlns:xsi[\S|\s]*?<\/articleDoc>', headline_text)
    for chunk in headline_chunks:
        try:
            h1 = re.search('<nitf:hedline>\s*<nitf:hl1>\s*(.*)\s*<\/nitf:hl1>', chunk).group(1)
        except:
            h1 = ''

        try:
            h2 = re.search('<nitf:hl2>\s*(.*)\s*<\/nitf:hl2>', chunk).group(1)
        except:
            h2 = ''

        try:
            aid = re.search('<dc:identifier identifierScheme="PGUID">urn:contentItem:(.*)<\/dc:identifier>', chunk).group(1)
        except:
            aid = ''

        try:
            pd = datetime.strptime(' '.join(re.search('<dateText>(.*)<\/dateText>', chunk).group(1).split(' ')[:-1]), '%B %d, %Y')
        except:
            pd = ''

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
        
        headline1.append(h1)
        headline2.append(h2)
        article_id.append(aid)
        source_filename.append(filename)
        pub_date.append(pd)
        news_wire.append(nw)
        language.append(lang)
        indexing_terms.append(curr_indexing_terms)
        company_terms.append(curr_company_terms)
        ticker_terms.append(curr_ticker_terms)

print(headline1)
print(headline2)
print(article_id)
print(source_filename)
print(pub_date)
print(news_wire)
print(language)
print(indexing_terms)
print(company_terms)
print(ticker_terms)
