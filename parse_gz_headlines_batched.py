import gzip
import regex as re
import os
import sys
from datetime import datetime
import sqlite3
import time
import zlib
import time
import pandas as pd
from pathlib import Path

# gz_files = ['3d9e6930-e8c2-4827-86b0-40642303cf16.gz', '4c98f145-b16f-4cd4-887a-a79a3f0d4fd1.gz', '4ed6ea26-7a98-40f7-8811-1d48a2da345f.gz', '5d5b744c-059d-494c-918f-c091ac429d76.gz', '06bd6e27-99a5-4cca-b64f-5916613502ac.gz', '7f79cb1a-d26d-4144-9b4b-2de32faee1e9.gz', '9abda55f-e4c1-44ca-a150-098492c12d42.gz', '42d25e59-c271-40ba-bf48-3e3173a4905e.gz', '58a41c1c-c766-4a4d-b456-40cfa5b57a79.gz', '182e73ea-f098-41f2-abba-17f3e5cad9f6.gz', '221c1e41-76ed-40b6-a5bf-2e10d1e55f53.gz', '378e856c-7703-40bc-958f-f29a39695187.gz', '0550eda1-96c0-4d8f-a88c-269fb9f32289.gz', '795ad137-d549-413d-85d0-1f5f084dcbfa.gz', '831ff016-df84-42b4-8b43-9cb73b88a236.gz', '900f7e82-6a60-463d-9eaa-42988af60781.gz', '918b4fa5-6bf1-4e5f-b435-f221287a15d9.gz', '1119f578-80ed-46c8-8de4-c3255de3e1e7.gz', '1666cce8-bbb6-4047-9ac9-833d46102f8d.gz', '2356fd67-9297-4814-80bf-efe83e0ec77f.gz', '4391ab6e-e44f-4dcb-9cda-60cfcda0ec26.gz', '9624e9d0-654d-441f-a387-45df41472756.gz', '57966c25-33c5-425c-86d1-6ebc603efbe4.gz', '73007ddc-7e98-4902-8aea-a7493b052b58.gz', 'a51abd73-4880-4d08-bbf1-7b89f2f8bfcc.gz', 'a289e97f-8d1f-4d37-ac1f-be74e50e27b0.gz', 'a74737c1-fa15-47c4-9505-6b21a1515b5d.gz', 'ae3c1677-34da-47c1-8083-3931373e4de5.gz', 'afe1ef94-1a78-4eeb-852a-1f2d615befb0.gz', 'b6bcb09b-567e-41bd-ab17-6c65998495cb.gz', 'b664faf9-7405-4bff-9ee0-5af450a96def.gz', 'bc472597-7815-4a52-bc83-7676866d5ca9.gz', 'be5a55fe-99da-41b7-b8ed-e2485cb7069c.gz', 'c2dbd97e-f508-409e-93e2-c07effd35b18.gz', 'c9de348d-62a8-488a-b4ee-a5d7d05877c8.gz', 'c37fe7f3-49d0-4d7d-a54a-c76d2d356519.gz', 'c82d3ba9-f067-42da-8112-98143361c461.gz', 'd0250d02-8e27-48fd-983d-cd2b83c83111.gz', 'dbf8cd8c-95f3-4e25-957c-77759ce6d7f4.gz', 'e90e0d5a-8292-4e25-aa62-fe62d22a175a.gz', 'ea0d6464-dbcc-4994-940f-28e76700c767.gz', 'ee37cc00-bb06-4285-b502-444a7120dd28.gz', 'f5e295a6-270b-4457-a25d-7cf05a8ad88c.gz', 'f7055551-e503-4cb1-9b8f-eae9433fe564.gz']

folder = sys.argv[1]
gz_files = [f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]

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

unprocessed_files = []
for file_path in gz_files:
    filename = '/home/ec2-user/press_release_data/' + file_path[:3] + '/' + os.path.basename(file_path).strip()
    headline_chunks = []
    file_size = os.path.getsize(filename)
    print(filename, os.path.getsize(filename))
    if file_size > 20 * 10**6:
        unprocessed_files.append(file_path)
        continue
    with gzip.open(filename, 'rt') as headline_dump:
        curr_chunk = ''
        headline_text = headline_dump.read()
        headline_chunks = re.findall('<articleDoc xmlns:xsi[\S|\s]*?<\/articleDoc>', headline_text)
        headline_text = ''
    
    num_headlines = len(headline_chunks)
    num_chars = 0
    num_words = 0
    for curr_chunk in headline_chunks:
        # handle curr chunk
        try:
            body = chunk_slice(curr_chunk, '<nitf:body.content>', '</nitf:body.content>')
            num_chars += len(body)
            num_words += len(body.split(' '))
        except: 
            pass

        try:
            h1 = chunk_slice(curr_chunk, '<nitf:hl1>', '</nitf:hl1>')
        except:
            h1 = ''

        try:
            h2 = chunk_slice(curr_chunk, '<nitf:hl2>', '</nitf:hl2>')
        except:
            h2 = ''

        try:
            aid = re.search('<dc:identifier identifierScheme="PGUID">urn:contentItem:(.*)<\/dc:identifier>', chunk).group(1)
        except:
            aid = ''

        try:
            curr_pub_date = datetime.striptime(' '.join(chunk_slice(curr_chunk, '<dateText>', '</dateText>').split(' ')[:-1]), '%B %d, %Y')
        except:
            curr_pub_date = ''

        try:
            nw = chunk_slice(curr_chunk, '<publicationName>', '</publicationName>')
        except:
            nw = ''

        try:
            lang = re.search(lang_regex, curr_chunk).group(1) 
        except:
            lang = ''

        try:
            indexing_chunk = re.search(indexing_chunk_regex, curr_chunk).group(1)
            curr_indexing_terms = re.findall(indexing_term_regex, indexing_chunk)
        except:
            curr_indexing_terms = []

        curr_it = ['{}:{}'.format(i[0], i[1]).replace("\"", "").replace("'", "") for i in curr_indexing_terms]

        try:
            h1_hash = str(hex(zlib.crc32(str.encode(h1)) & 0xffffffff))
            h2_hash = str(hex(zlib.crc32(str.encode(h2)) & 0xffffffff))
            pub_date_hash = str(hex(zlib.crc32(str.encode(curr_pub_date)) & 0xffffffff))
        except Exception as e:
            h1_hash = ''
            h2_hash = ''
            pub_date_hash = ''
            print('failed to complete row {} due to {}'.format(i, repr(e)))

        cmd = '''
            INSERT INTO headline_data(headline_1, headline_1_hash, headline_2, headline_2_hash, article_id, newswire, publication_date, publication_date_hash, language, indexing_terms, source_file)
            VALUES
                ("{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}")
        '''.format(h1, h1_hash, h2, h2_hash, aid, nw, curr_pub_date, pub_date_hash, lang, curr_it, file_path)
        c.execute(cmd)
        
        # track file / folder metadata (e.g. file => # headlines in file)
        

    conn.commit()
unprocessed_files_df = pd.DataFrame({'unprocessed_files': unprocessed_files})
unprocessed_files_path = Path("./unprocessed_files.csv")
if unprocessed_files_path.is_file():
    existing_df = pd.read_csv('./unprocessed_files.csv')
    existing_df = existing_df.append(unprocessed_files_df)
    existing_df.to_csv('./unprocessed_files.csv')
else:
    unprocessed_files_df.to_csv('./unprocessed_files.csv')
