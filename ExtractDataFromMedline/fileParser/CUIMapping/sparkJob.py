import pickle

from pyspark import SparkContext,SparkConf
import commonFunctions as cf
import mapper as mp



mylogger = cf.MMOLogger().getLogger(__name__,'./log/sparkJob.txt')

conf = SparkConf().setMaster('172.16.150.241')\
             .setAppName("My application")\
             #.set("spark.executor.memory", "1g")

sc = SparkContext('local','fileMapper')

def splitLine(line):
    linel = line.split('|')
    yy = linel[1]
    pm = linel[0]
    country = linel[3]
    aff = linel[4]
    return yy,(pm,country,aff)

def to_list(a):
    return [a]

def append(a, b):
    a.append(b)
    return a

def extend(a, b):
    a.extend(b)
    return a


def getPMIDdataFROMSRC2(pmID, pathURL='/media/gaurav/Elements/Thesis/data/MMO/CompleteData/files/processed/'):
    for dicts in cf.getAllfiles(pathURL):
        with open(pathURL + dicts, 'rb') as fin:
            dictipickle = pickle.load(fin)
            if pmID in dictipickle.keys():
                return pmID,dictipickle[pmID]


if __name__=='__main__':


    """
    data = sc.parallelize([("A",1),("A",2),("B",1),("B",2),("C",1)] )
>>> data.combineByKey(lambda v : str(v)+"_", lambda c, v : c+"@"+str(v), lambda c1, c2 : c1+c2).collect()
    """

    datafile = sc.textFile('/media/gaurav/Elements/Thesis/src/ExtractDataFromMedline/Downloader/all/top20Records.txt')
    yearDict = datafile.map(lambda line: splitLine(line)).combineByKey(to_list,append,extend).collectAsMap()
    print('dictionary containing year as key created and has %d unique key(s)', len(yearDict))
    pmIDList = datafile.map(lambda line: splitLine(line)[1][0])
    pmIDdict= pmIDList.map(lambda pmID:mp.getPMIDdataFROMSRC2(pmID))

    #print(pmIDList.collect())







