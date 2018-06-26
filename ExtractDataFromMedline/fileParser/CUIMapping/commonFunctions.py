"""
This module contains list of generic functions
"""
import os
import pickle

"""
Author : Gaurav Vashisth, date:18:06:2018
"""
import logging



class MMOLogger():
    def getLogger(self,loggerName,loggerURL):
        #self.loggerConfig = loggerConfig
        self.loggerName = loggerName
        self.loggerURL = loggerURL

        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(self.loggerName)
        hdlr = logging.FileHandler(self.loggerURL)
        formatter = logging.Formatter('%(asctime)s %(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.setLevel(logging.INFO)

        return logger




def getAllfiles(path, startStr=''):
    """
    :param path: URL of the folder
    :startStr: starting text if specific named files are needed
    :return: a list of all files inside a folder

    """
    if startStr!= '':

        return [ y   for x in (os.walk(path)) for y in x[2] if y.startswith(startStr)  ]
    else:
        return [y for x in (os.walk(path)) for y in x[2] ]


def filepathExist(filepath):

    return os.path.isfile(filepath), True, False


def createFile(filename,fileURL):
    os.mknod(fileURL+filename)
    print('file %s created at %',filename,fileURL)


def saveDictionary(fileObject,filepath):
    with open(filepath,'wb') as fout:
        pickle.dump(fileObject,fout, pickle.DEFAULT_PROTOCOL)


def loadDictionary(filepath):
    with open(filepath,'rb') as fin:
        dicti = pickle.load(fin)
    return dicti


