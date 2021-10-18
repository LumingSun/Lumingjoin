# %%
from numpy.core.arrayprint import set_string_function
import pandas as pd
import numpy as np

import json
from collections import Counter

import torch
import torch.nn as nn
import torch.optim as optim
import torch.nn.functional as F

from blitz.modules import BayesianLSTM
from blitz.utils import variational_estimator

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

# node_type, sort keys[bag], join condition[bag?], 
# condition_filter[bag], condition_index[bag?], index_name, est_card, 
# cost, latency


# %%
with open("./transformed_sequence.json", "r") as f:
    sequences = json.load(f)
# %%
for sequence in sequences:
    for ts in sequence:
        if(ts[3]!='None'):
            # print(sequence)
            print(ts[3])
# sequences[0]
# %%
# collect words to construct dictionary
words = []
# TODO
# complex way to get words (need to update in encoding processing) LIKE ANY 
# split node type into bag
def flatten_list(l,words):
    for each in l:
        if isinstance(each,list):
            words = flatten_list(each,words)
        elif isinstance(each,str):
            words.append(each)
    return words

for sequence in sequences:
    words = flatten_list(sequence,words)

    
vocab = Counter(words) # create a dictionary
vocab = sorted(vocab, key=vocab.get, reverse=True)
vocab_size = len(vocab)
word2idx = {word: ind for ind, word in enumerate(vocab)} 

# %%

# %%

# %%

# %%

# %%

# %%
