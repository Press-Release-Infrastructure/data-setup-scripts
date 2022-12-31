import pandas as pd 
import nltk
from nltk.tag import brill, brill_trainer
from nltk.tag import DefaultTagger, UnigramTagger, BigramTagger, TrigramTagger, BrillTaggerTrainer
from nltk.corpus import treebank
import pickle

retrain_val = False

def get_initial_train_test():
    tagged_sents = treebank.tagged_sents()
    train_data = tagged_sents[:3000]
    test_data = tagged_sents[3000:]
    return train_data, test_data

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

train_data, test_data = get_initial_train_test()
brill_tag = get_model(retrain = retrain_val)
print(brill_tag.accuracy(test_data))

# a = initial_tag.accuracy(test_data)
# print ("Accuracy of Initial Tag : ", a)

# use initial tagger to identify POS of other words in crunchbase headlines, while company names are fuzzy-matched as ACQ, ACD (2 new POS)


# split tagged crunchbase headlines into train + test set with POS per word
# select a randomized large subset of LN headlines, run tagger 
# if ACQ, AQD tags both present in tagged headline, mark as acquisition

# def create_train_test():
#     acq_cb = pd.read_csv('acq_cb.csv')
#     non_acq_cb = pd.read_csv('non_acq_cb.csv')

# train_X, train_y, test_X, test_y = create_train_test()
# print(train_X[:5], train_y[:5])
