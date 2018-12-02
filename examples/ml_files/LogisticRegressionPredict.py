import sklearn.linear_model
import pandas as pd
import numpy as np
import pickle

row = pd.read_csv('predictrow.csv', sep=',', header=None).values
row = row[:, 1:]
clf = pickle.load(open('example.model', 'rb'))
pred = clf.predict(row)
print(pred)
