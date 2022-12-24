import numpy as np 
import sqlite3
import pandas as pd

conn = sqlite3.connect('/home/ec2-user/data_setup_scripts/press_release_headlines.db', sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
c = conn.cursor()

non_acq_avg = np.array([0.23528043890366043, 0.14271531549544042, 0.22219608491740156, 0.2496682778164644, 0.0689980148332913, 0.011367251171130738, -0.47286722250747826, -0.37478603391485865, 0.16688423277705058, 0.10458102360017907, 0.07406869085007305, 0.04050418110279025, -0.1818404985425903, -0.04582891975959258, 0.16731644620880598, 0.06610345213669257, -0.12602966011476685, 0.1132894871523825, -0.18484592614154877, -0.16197002744323555, 0.3957926050884899, -0.03265945974101817, -0.05477679171502624, 0.01523605543830307, -0.13843153244980383, -0.9708679124813309, -0.1539930908701521, -0.1953623864743327, -0.0756280876205966, -0.0724946125378628, 2.436652402199323, 0.11001662670355129, -0.13595486950945598, -0.18081731500590256, -0.034757455731503184, -0.10192835143416788, -0.07371519003471011, 0.05132736788521595, 0.014249690529789657, -0.28762456142983556, 0.09528370726116847, -0.09817043269951242, -0.011869337121235477, 0.023530255885804357, -0.09343273721348454, 0.06216924654959376, -0.047636060901114705, 0.1310496523990785, 0.005385410535573008, 0.10394545563240179])
acq_avg = np.array([0.3559818161667853, -0.034628441647341274, 0.3672835000621112, 0.4688803689911933, 0.13739581809829468, 0.06739564864782123, -0.45799789517609735, -0.440019135949745, 0.2521444055954233, 0.2615601484298768, 0.28233744623833057, 0.1848686081182513, -0.13342346585353748, -0.12887203232920097, -0.06660552466308838, 0.22736043725522237, -0.22051776089130817, 0.23465800042154147, -0.09348971437664184, -0.16354186440582266, 0.515283533214146, -0.017816992151456146, -0.22935054106632954, -0.03534342400800169, -0.2576813398529104, -0.8044992037652229, -0.19358205996718222, -0.2729088278986107, 0.04851127073605692, -0.11809366810580296, 1.9981094110896938, 0.030327884332457956, -0.04363365242210034, -0.10676299253543443, -0.03274388274360803, -0.23801312143937367, -0.3090027892692571, 0.05127489134449213, 0.141293915491224, -0.32132929995225923, 0.3583839910623991, -0.22805940183126075, 0.08032797197965745, 0.17245425361269362, -0.15747963523062128, 0.10877852281158522, -0.04981786860248397, 0.2510978175826959, -0.04030956726002803, 0.1440323412278314])

# embeddings = {}
# with open('glove.6B.50d.txt') as f:
#     lines = f.read().splitlines()
#     for l in lines:
#         split_l = l.split(' ')
#         word = split_l[0]
#         vec = np.array(split_l[1:]).astype(float)
        
#         embeddings[word] = vec

# def calc_headline_vec(headline):
#     vecs = []
#     headline_split = headline.split(' ')
#     for h in headline_split:
#         clean_h = h.strip().lower()
#         if clean_h in embeddings:
#             vecs.append(embeddings[clean_h])
#     if not len(vecs):
#         return []
#     vec_avg = np.array(vecs).mean(axis = 0)
#     return vec_avg

# c.execute("SELECT headline_1 FROM headline_data limit 5")
# headlines = [i[0] for i in c.fetchall()]
# for h in headlines:
#     print(h, calc_headline_vec(h))

# find a set of headlines with unique article id
c.execute('select * from headline_data group by article_id having count(*) = 1 limit 300000')
headlines = c.fetchall()

headline_1 = []
headline_2 = []
article_id = []
pub_date = []
database_id = []

for h in headlines:
    headline_1.append(h[1])
    headline_2.append(h[2])
    article_id.append(h[3])
    pub_date.append(h[5])
    database_id.append(h[11])

unique_aid_headlines = pd.DataFrame({'headline_1': headline_1, 'headline_2': headline_2, 'database_id': database_id, 'article_id': article_id, 'pub_date': pub_date})
unique_aid_headlines.to_csv('unique_aid_headlines.csv')

print(headlines[:5])