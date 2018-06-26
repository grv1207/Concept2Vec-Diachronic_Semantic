"""
This script parses an MMO output that we get from https://mbr.nlm.nih.gov/Download/MetaMapped_Medline/2014/ which is first
partially processed in bash to get utterance and mappings fields

the object of MMO would process each file in given directoy in parallel and would store their result in a dictionary
if the size of dictionary is more than  threshold <3 GB> it would spill out the dictionary in a pickle form
"""
import json
import logging
import pickle
import sys
import os
import subprocess
import multiprocessing as mp

"""
Author : Gaurav Vashisth, date:15:05:2018
"""

import re
from collections import OrderedDict


logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)
hdlr = logging.FileHandler('../Downloader/log/MMO.log')
formatter = logging.Formatter('%(asctime)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

def parseFile(fileName, destination='/media/gaurav/Elements/Thesis/data/MMO/CompleteData/files/processed/'):
    pmDict = OrderedDict()
    srcdir = "/media/gaurav/Elements/Thesis/data/MMO/CompleteData/files/semiprocessed/"
    srcFile = srcdir+fileName
    #filL = fileName.split('.')[2]
    destFileURL = destination+fileName+'.bin'
    logger.info(fileName+'  started')
    if not os.path.isfile(destFileURL):
        with open(srcdir+fileName,'r') as fin:
            for i,line in enumerate(fin):
                if line.startswith('utterance'):
                    line = line.replace('""','')
                    lineParts = line.split('"')
                    if (len(lineParts)>1):
                        pmID = lineParts[0].split("(")[1].replace("\'","").split('.')[0]
                        sentenceNo = '.'.join(lineParts[0].split("(")[1].replace("\'","").replace(',','').split('.')[1:3])
                        sentence = sentenceNo+' | '+lineParts[1]
                        """
                            take care of offset ofr each concept and store them in a onject
                            create list for concepts"""
                        mainOffset = int(lineParts[2].split(',')[1].split('/')[0])
                        if pmID not in pmDict:
                            pmDict[pmID] = OrderedDict()

                        pmDict[pmID][sentence] = OrderedDict()
                if line.startswith('mappings'):
                    line = line.replace("mappings(","").replace(").",",")
                    firstSplit = line.split("map(")[1:]
                    for sublines in firstSplit:
                        secondSplits = sublines.split("ev(")[1:]
                        for parts in secondSplits:
                            """pmDict[pmID][title].append(parts.split(',')[1].replace("\'",""))
                            pmDict[pmID][title].append(parts.split(',')[-4])"""
                            cuiList = parts.split(',')[2].replace("\'", "").split(' ')
                            cuiName = '_'.join(cuiList)
                            #cuiName = re.sub('[^0-9a-zA-Z]+ ', '', cuiName)
                            cuid = parts.split(',')[1].replace("\'","")
                            offset =  hasNumbers(parts.replace('[','').replace(']','').split(',')[int(-4-len(cuiList)+1):][:len(cuiList)],mainOffset,pmID,srcFile)

                            if cuiName not in list(pmDict[pmID][sentence].keys()):
                                pmDict[pmID][sentence][cuiName] = []
                                pmDict[pmID][sentence][cuiName].append(cuid)
                                pmDict[pmID][sentence][cuiName].append(offset)

        logger.info(fileName+' : '+str(len(pmDict)))





        with open(destFileURL,'wb') as dictOut:
           pickle.dump(pmDict,dictOut,protocol=pickle.HIGHEST_PROTOCOL)
           logger.info(destFileURL+' dumped')

    else:
        logger.info(fileName+' exists ')

    return str(len(pmDict))

def hasNumbers(inputList,mainOffset,pmID, fileName):
    newList = []
    #if pmID=='5807301':
        #print(pmID)
    for sublist in inputList:
        intCount = sublist.replace('/','')
        if len(intCount)>=3:
            if intCount.isdigit():
                subOffsetList = sublist.split('/')
                try:
                    offsetTup =(int(subOffsetList[0]) - mainOffset, int(subOffsetList[1]))
                    newList.append(offsetTup)
                except Exception as e:
                    logger.info(str(fileName)+' : '+str(pmID)+' : '+str(e))
    return newList
def getAllfiles(path):
    """
    returns a list of all files inside a folder
    :param path:
    :return:

    """

    return [ y   for x in (os.walk(path)) for y in x[2] if y.startswith('sprc')  ]

def  useBash(file):
    folder = '/media/gaurav/Elements/Thesis/data/MMO/'
    subprocess.call([' sh process.sh', file,folder])



if __name__ == '__main__':


    #lisOfrawFiles = getAllfiles('/media/gaurav/Elements/Thesis/data/MMO/')

    """
    get the list of all semi parsed file here and apply multiprocess thread here!!!
    """
    lisOfprcFiles = getAllfiles('/media/gaurav/Elements/Thesis/data/MMO/CompleteData/files/semiprocessed/')

    p = mp.Pool(10)
    print(p.map(parseFile, lisOfprcFiles))
    #for file in lisOfprcFiles:
        #parseFile(file)


    #for maink,subs in dicti.items():
        #for k,v in subs.items():

            #print(maink+' : '+str(k))
        #or subl in v :
            #print(k+' : ' +str(subl[0])+' :  '+str(subl[1:]))"""

    #print(dicti['23968992'])
    #print(dicti['24250098'])
    """with open('C:\\Users\\gava01\\Documents\\Gaurav\\thesis\\fileParser\\processedFolder\\out_01.bin', 'rb') as dictOut:
        dicti = pickle.load(dictOut)
    print(len(dicti))
    for k,i in dicti.items():
        #print(k,':',i)
        for a,b in i.items():
             print(k,'|',a,'|',type(b))"""
