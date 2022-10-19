import gzip
import regex as re
import os
from datetime import datetime

file_path = '9ec7c097-473e-41f3-b890-cdcd45d1d0ac.gz'
headline1 = []
headline2 = []
article_id = []
source_filename = []
pub_date = []
news_wire = []

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
        
        headline1.append(h1)
        headline2.append(h2)
        article_id.append(aid)
        source_filename.append(filename)
        pub_date.append(pd)
        news_wire.append(nw)

print(headline1)
print(headline2)
print(article_id)
print(source_filename)
print(pub_date)
print(news_wire)
