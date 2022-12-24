import sqlite3
import pandas as pd
import zlib

conn = sqlite3.connect('/home/ec2-user/data_setup_scripts/press_release_headlines.db', sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
c = conn.cursor()

files = ['00a4409a-29f4-4b6d-8322-cb8eaaa2a2f2',
'0fd08174-0f9d-42aa-8984-d1231ab0b944',
'1deee14c-c43e-442d-9fa0-142c0176d6f3',
'2cb682a3-4b36-46cf-bc03-6e71713c180f',
'2d335fee-a32a-4675-bf8e-00011f133a9f',
'3d9e6930-e8c2-4827-86b0-40642303cf16',
'4c98f145-b16f-4cd4-887a-a79a3f0d4fd1',
'4ed6ea26-7a98-40f7-8811-1d48a2da345f',
'5d5b744c-059d-494c-918f-c091ac429d76',
'06bd6e27-99a5-4cca-b64f-5916613502ac',
'6dd5e0cb-6f29-406f-b9d4-7d7ecfc46f9a',
'7d8e5b48-78e2-45ba-89c9-a10931c40be6',
'7f79cb1a-d26d-4144-9b4b-2de32faee1e9',
'08e53cab-3258-428c-b059-14cf3f6def9a',
'9abda55f-e4c1-44ca-a150-098492c12d42',
'9df467ad-49b1-4f38-a1da-9e93a6d18cde',
'35aa53cb-e732-4cef-b570-8ea47b6ad8c8',
'42d25e59-c271-40ba-bf48-3e3173a4905e',
'58a41c1c-c766-4a4d-b456-40cfa5b57a79',
'95e1c955-94ae-4dcf-8456-84f3d448353c',
'182e73ea-f098-41f2-abba-17f3e5cad9f6',
'221c1e41-76ed-40b6-a5bf-2e10d1e55f53',
'328bb652-323e-46ba-a963-dd840e6ae786',
'378e856c-7703-40bc-958f-f29a39695187',
'0550eda1-96c0-4d8f-a88c-269fb9f32289',
'795ad137-d549-413d-85d0-1f5f084dcbfa',
'831ff016-df84-42b4-8b43-9cb73b88a236',
'900f7e82-6a60-463d-9eaa-42988af60781',
'918b4fa5-6bf1-4e5f-b435-f221287a15d9',
'1119f578-80ed-46c8-8de4-c3255de3e1e7',
'1666cce8-bbb6-4047-9ac9-833d46102f8d',
'2356fd67-9297-4814-80bf-efe83e0ec77f',
'4391ab6e-e44f-4dcb-9cda-60cfcda0ec26',
'6501d061-92f8-4203-9ba4-13b8dc347ad3',
'9624e9d0-654d-441f-a387-45df41472756',
'57966c25-33c5-425c-86d1-6ebc603efbe4',
'73007ddc-7e98-4902-8aea-a7493b052b58',
'907836fa-c8b8-4f2c-847a-0de93509ed26',
'2688731f-bf75-4b6e-8551-2c67b4d1514b',
'37138040-d876-4a6c-83b3-d7a82d59a9f7',
'80897898-49f4-40e5-a14a-0689f0589a06',
'a51abd73-4880-4d08-bbf1-7b89f2f8bfcc',
'a289e97f-8d1f-4d37-ac1f-be74e50e27b0',
'a74737c1-fa15-47c4-9505-6b21a1515b5d',
'a361643b-3d2c-4bb5-b3de-5c07aef19f0a',
'ab7690f0-2e8b-4660-a790-109f79ec10fc',
'ad14a20c-8643-4446-b3ee-394603efd40a',
'ae3c1677-34da-47c1-8083-3931373e4de5',
'afe1ef94-1a78-4eeb-852a-1f2d615befb0',
'b6bcb09b-567e-41bd-ab17-6c65998495cb',
'b664faf9-7405-4bff-9ee0-5af450a96def',
'bc472597-7815-4a52-bc83-7676866d5ca9',
'be5a55fe-99da-41b7-b8ed-e2485cb7069c',
'c2dbd97e-f508-409e-93e2-c07effd35b18',
'c9de348d-62a8-488a-b4ee-a5d7d05877c8',
'c37fe7f3-49d0-4d7d-a54a-c76d2d356519',
'c82d3ba9-f067-42da-8112-98143361c461',
'd0250d02-8e27-48fd-983d-cd2b83c83111',
'da67db3b-dca8-4fcc-afc1-616e6b7fd8f0',
'dbf8cd8c-95f3-4e25-957c-77759ce6d7f4',
'e4b87aa3-0741-436a-ba8a-420700e450fb',
'e90e0d5a-8292-4e25-aa62-fe62d22a175a',
'ea0d6464-dbcc-4994-940f-28e76700c767',
'ee37cc00-bb06-4285-b502-444a7120dd28',
'f5e295a6-270b-4457-a25d-7cf05a8ad88c',
'f5207717-5f8f-4a66-8514-be0a4070424f',
'f7055551-e503-4cb1-9b8f-eae9433fe564']
files_to_recompute = []

for i in files:
    filename = "{}.gz".format(i)
    c.execute('select * from headline_data where source_file = \"{}\"'.format(filename))
    result = c.fetchall()
    if len(result) == 0:
        files_to_recompute.append(filename)

print(files_to_recompute)
