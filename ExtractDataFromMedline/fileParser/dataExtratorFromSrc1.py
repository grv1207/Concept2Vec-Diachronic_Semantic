"""
This script is creating a dictionary from Source 1 data where pubmed year is dict and pubmedID, Country of publication and Affliation are its value
The output of this script is one big dictionary that has all the information from year 2002 - 2017

"""
import logging
import pickle
import sys
import os
import subprocess
import multiprocessing as mp
from macpath import join

#import findspark
#findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
"""
Author : Gaurav Vashisth, date:12:06:2018
"""

import re
from collections import OrderedDict
import gzip

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)
hdlr = logging.FileHandler('./log/SRC1.log')
formatter = logging.Formatter('%(asctime)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)





def dictCreater(filepath):
    """
    :param filepath: path of file that has complete data from 2002-2017
    :return: pickle file having year as key and pubmedID, Country of publication and Affliation as values
    """
    sourceOneDict = dict()
    count = 0
    with open(filepath,'r') as fin :
        for i,line in enumerate(fin):
            if(i>290000000):
                try:
                    k_ = int(line.split('|')[1]) # year

                    if len(line.split('|')) >=5:
                        v_ = [line.split('|')[0],line.split('|')[-2],line.split('|')[-1].replace('\n','')]

                    elif len(line.split('|'))==3 :   # when pmid, year and title present
                       # logger.info('line with length 3 '+line)
                        v_ = line.split('|')[0]

                    elif len(line.split('|'))==4 : # when pmid, year , title and country of origin present
                        logger.info('line with length 4 ' + line)
                        v_ = [line.split('|')[0],line.split('|')[-1].replace('\n','')]


                    if k_ not in sourceOneDict:
                        sourceOneDict[k_] = []

                    sourceOneDict[k_].append(v_)
                except(Exception ) as e :
                    logger.error('error: %s, line: %s, lineNo : %s',e,line,i)

                #if i%10000000 ==0 and i!=0 :

                    #count = count+i
        logger.info('%s records completed',i)
        with open(str(i)+'.bin','ab') as fout:
            pickle.dump(sourceOneDict,fout,pickle.HIGHEST_PROTOCOL)
        logger.info('10000000  records dumped')
        sourceOneDict.clear()


def sortfile(filpath):
    conf = SparkConf().setMaster("local").setAppName("sorting")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    datafile = sc.textFile(filpath).sortBy(lambda x: fileSpliter(x))\
        .map(lambda x: createKeyValue(x))

    #print(datafile.collect())
    #dfRDD = sqlContext.createDataFrame(datafile)


    """ .map(lambda x: createKeyValue(x))\
        .combineByKey(lambda listX: list(listX),
                      lambda x,listX : x+ listX,
                      lambda x,y : x+y
                      )"""
    #print(datafile.collect())
    datafile.repartition(1).saveAsTextFile('src1Data/complete')
    #dfRDD.coalesce(1).write.format("com.databricks.spark.csv").save('src1Data/complete')


def stringJoin(listX):
    #templist = [listX.split('|')[1],listX.split('|')[0]]
    #templist.append(listX.split('|')[1:])
    return '|'.join( str(x) for x in listX.split('|'))
def fileSpliter(x):
    if len(x)>=2:
        return x.split('|')[1]
    else:
        logger.error(x)



def createKeyValue(listX):
    try:
        k_ = str(listX.split('|')[1])  # year

        if len(listX.split('|')) >= 5:
            v_ = [str(listX.split('|')[0]), listX.split('|')[-2], listX.split('|')[-1].replace('\n', ' ')]

        elif len(listX.split('|')) == 3:  # when pmid, year and title present
            # logger.info('line with length 3 '+line)
            v_ = [str(listX.split('|')[0]),'','']

        elif len(listX.split('|')) == 4:  # when pmid, year , title and country of origin present
            #logger.info('line with length 4 ' + listX)
            v_ = [str(listX.split('|')[0]), listX.split('|')[-1].replace('\n', ''),'']

    except(Exception) as e:
        logger.error('error: %s, line: %s', e, listX)
    #print(len(v_))
    keyValue = v_[:]
    keyValue.insert(0,k_)
    #print(v_)
    #print('|'.join(keyValue))
    return '|'.join(keyValue)

if __name__ == '__main__':
    fileURL = '/media/gaurav/Elements/Thesis/src/ExtractDataFromMedline/Downloader/all/complete.txt'
    #dictCreater(fileURL)
    sortfile(fileURL)


