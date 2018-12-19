import pip
import pip._internal

def install(package):
    if hasattr(pip, 'main'):
        pip.main(['install', package])
    else:
        pip._internal.main(['install', package])

install("scikit-learn")
install("pandas")

from sklearn.linear_model import LogisticRegression
import sys
import numpy as np 
import pandas as pd
import pickle

datapath = sys.argv[1]

dataset = pd.read_csv(datapath, sep=',').values
X = dataset[:,1:]
y = dataset[:,0]
model = LogisticRegression().fit(X, y)

model_file = open('example.model', 'wb')
pickle.dump(model, model_file)
print('Model trained finished.')
