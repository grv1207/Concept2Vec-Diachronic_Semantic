import os
import pickle

def getAllfiles(path):
    """
    returns a list of all files inside a folder
    :param path:
    :return:

    """

    return [ y   for x in (os.walk(path)) for y in x[2] if y.startswith('sprc')  ]


completeDict = dict()
url = '/media/gaurav/Elements/Thesis/data/MMO/CompleteData/files/processed/'
for dicts in getAllfiles(url):
    with open(url+dicts,'rb') as fin:
        dictipickle = pickle.load(fin)
        completeDict.update(dictipickle)

with open('src2Data/completeDict', 'wb') as fout:
    pickle.dump(completeDict, fout, pickle.HIGHEST_PROTOCOL)