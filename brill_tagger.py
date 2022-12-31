import pandas as pd 
import nltk
from nltk.tag import brill, brill_trainer
from nltk.tag import DefaultTagger, UnigramTagger, BigramTagger, TrigramTagger, BrillTaggerTrainer
from nltk.corpus import treebank
import pickle
import unicodedata
import string

retrain_val = False

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

# def add_crunchbase_acq_acd_tags(acq_headlines, acq_headlines_tagged, acquirer, acquired):
#     for a in acq_headlines_tagged:

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

# use initial tagger to identify POS of other words in crunchbase headlines
brill_tag = get_model(retrain = retrain_val)
acq_headlines, acquirer, acquired, non_acq_headlines = get_crunchbase_dataset()
acq_headlines_round1 = tag_untagged_headlines(brill_tag, acq_headlines)
non_acq_headlines_round1 = tag_untagged_headlines(brill_tag, non_acq_headlines)
print(acq_headlines_round1[:5])
print()
print(non_acq_headlines_round1[:5])

# fuzzy-match company names for acq headlines as ACQ, ACD (2 custom POS)

# split tagged crunchbase headlines into train + test set with POS per word
# select a randomized large subset of LN headlines, run tagger 
# if ACQ, AQD tags both present in tagged headline, mark as acquisition
