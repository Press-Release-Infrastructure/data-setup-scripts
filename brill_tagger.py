import pandas as pd 
import nltk
from nltk.tag import brill, brill_trainer
from nltk.tag import DefaultTagger, UnigramTagger, BigramTagger, TrigramTagger, BrillTaggerTrainer
from nltk.corpus import treebank
import pickle
import unicodedata
import string
from fuzzywuzzy import fuzz

retrain_val = False
retag = False

def check_phrase(phrase, target):
    return fuzz.ratio(target, phrase) > 90

def find_word_index(headline, first_word):
    i = 0
    for w in headline:
        if w == first_word:
            return i
        i += 1
    return -1

def check_headline(h, target, thresh):
    n = len(target.split(' '))
    headline_str = ' '.join(h)
    for len_chunk in range(n, 0, -1):
        combos = [' '.join(t) for t in zip(*(h[i:] for i in range(len_chunk)))]
        mod_target = ' '.join(target.split(' ')[:len_chunk])
        for phrase in combos:
            if check_phrase(phrase, mod_target):
                idx_start = find_word_index(h, phrase.split(' ')[0])
                return phrase, idx_start, len_chunk
    return '', -1, 0

def get_initial_train_test():
    tagged_sents = treebank.tagged_sents()
    train_data = tagged_sents[:3000]
    test_data = tagged_sents[3000:]
    return train_data, test_data

def clean_split_headline(headline):
    return list(filter(None, headline.translate(str.maketrans('', '', string.punctuation)).replace('�', '').strip().lower().split(' ')))

def get_crunchbase_dataset():
    acq_cb = pd.read_csv('acq_cb.csv')
    non_acq_cb = pd.read_csv('non_acq_cb.csv')

    acq_headlines = list(acq_cb['headline'].apply(clean_split_headline))
    acquirer = list(acq_cb['acquirer'].str.lower())
    acquired = list(acq_cb['acquired'].str.lower())
    non_acq_headlines = list(non_acq_cb['headline'].apply(clean_split_headline))
    return acq_headlines, acquirer, acquired, non_acq_headlines

def add_crunchbase_acq_acd_tags(acq_headlines, acq_headlines_tagged, acquirer, acquired, save_file):
    num_headlines = len(acq_headlines)
    tagged_headlines = []
    for i in range(num_headlines):
        curr_headline, curr_tagged, acq, acd = acq_headlines[i], acq_headlines_tagged[i], acquirer[i], acquired[i]
        
        if type(acq) == str:
            phrase, idx_start, len_chunk = check_headline(curr_headline, acq, 0.90)
            if idx_start >= 0:
                for offset in range(len_chunk):
                    curr_tagged[idx_start + offset] = (curr_tagged[idx_start + offset][0], 'ACQ')
        else:
            acq = ''
        
        if type(acd) == str:
            phrase, idx_start, len_chunk = check_headline(curr_headline, acd, 0.90)
            if idx_start >= 0:
                for offset in range(len_chunk):
                    curr_tagged[idx_start + offset] = (curr_tagged[idx_start + offset][0], 'ACD')
        else:
            acd = ''

        tagged_headlines.append(curr_tagged)

    acq_tagged = pd.DataFrame({'tagged': tagged_headlines, 'acquirer': acquirer, 'acquired': acquired})
    acq_tagged.to_csv(save_file)

def get_lexis_nexus_dataset():
    pass 

def backoff_tagger(train_sents, tagger_classes, backoff=None):
    for cls in tagger_classes:
        backoff = cls(train_sents, backoff=backoff)
    return backoff

def get_model(retrain = False):
    if retrain:
        train_data, test_data = get_initial_train_test()
        default_tagger = DefaultTagger('NN')
        initial_tag = backoff_tagger(
            train_data, [UnigramTagger, BigramTagger, 
                        TrigramTagger], backoff = default_tagger)

        brill_tag = train_brill_tagger(initial_tag, train_data)
        with open('sequential_backoff_tagger.pkl', 'wb') as fout:
            pickle.dump(brill_tag, fout)
        return brill_tag
    else:
        f = open('sequential_backoff_tagger.pkl', 'rb')
        brill_tag = pickle.load(f)
        f.close()
        return brill_tag

def train_brill_tagger(initial_tagger, train_sents, **kwargs):
    templates = [
            brill.Template(brill.Pos([-1])),
            brill.Template(brill.Pos([1])),
            brill.Template(brill.Pos([-2])),
            brill.Template(brill.Pos([2])),
            brill.Template(brill.Pos([-2, -1])),
            brill.Template(brill.Pos([1, 2])),
            brill.Template(brill.Pos([-3, -2, -1])),
            brill.Template(brill.Pos([1, 2, 3])),
            brill.Template(brill.Pos([-1]), brill.Pos([1])),
            brill.Template(brill.Word([-1])),
            brill.Template(brill.Word([1])),
            brill.Template(brill.Word([-2])),
            brill.Template(brill.Word([2])),
            brill.Template(brill.Word([-2, -1])),
            brill.Template(brill.Word([1, 2])),
            brill.Template(brill.Word([-3, -2, -1])),
            brill.Template(brill.Word([1, 2, 3])),
            brill.Template(brill.Word([-1]), brill.Word([1])),
            ]
      
    # Using BrillTaggerTrainer to train 
    trainer = brill_trainer.BrillTaggerTrainer(
            initial_tagger, templates, ruleformat = 'str', deterministic = True)
      
    return trainer.train(train_sents, **kwargs)

def tag_untagged_headlines(tagger, headlines):
    tagged_headlines = []
    for h in headlines:
        tagged_headlines.append(tagger.tag(h))
    return tagged_headlines

def get_acq_non_acq_cb_headlines(retag = False):
    if retag:
        # use initial tagger to identify POS of other words in crunchbase headlines
        brill_tag = get_model(retrain = retrain_val)

        acq_headlines, acquirer, acquired, non_acq_headlines = get_crunchbase_dataset()
        acq_headlines_round1 = tag_untagged_headlines(brill_tag, acq_headlines)
        non_acq_headlines_round1 = tag_untagged_headlines(brill_tag, non_acq_headlines) 

        # save non acquisition headlines to separate file
        non_acq_headlines_round1 = pd.DataFrame({'tagged': non_acq_headlines_round1})
        non_acq_headlines_round1.to_csv('non_acq_headlines_tagged.csv')

        # fuzzy-match company names for acq headlines as ACQ, ACD (2 custom POS)
        add_crunchbase_acq_acd_tags(acq_headlines, acq_headlines_round1, acquirer, acquired, 'acq_headlines_tagged.csv')
    acq_headlines_tagged = list(pd.read_csv('acq_headlines_tagged.csv')['tagged'])
    non_acq_headlines_tagged = list(pd.read_csv('non_acq_headlines_tagged.csv')['tagged'])
    return acq_headlines_tagged, non_acq_headlines_tagged

acq_headlines_tagged, non_acq_headlines_tagged = get_acq_non_acq_cb_headlines(retag = False)

# split tagged crunchbase headlines into train + test set with POS per word

# select a randomized large subset of LN headlines, run tagger 
# if ACQ, AQD tags both present in tagged headline, mark as acquisition
