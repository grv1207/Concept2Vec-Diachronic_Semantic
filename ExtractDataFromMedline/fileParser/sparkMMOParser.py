



import re
from collections import OrderedDict
from pyspark import SparkContext, SparkConf
spark_conf = SparkConf().setAppName("MMO parser")
sc = SparkContext(master='local' ,conf=spark_conf)

#print(sc._conf.getAll())
def _parseFile(line, pmDict):
    print(line)

    if line.startswith('utterance'):
        line = line.replace('""','')
        lineParts = line.split('"')
        pmID = lineParts[0].split("(")[1].replace("\'","").split('.')[0]
        sentence = lineParts[1]
        sentence = re.sub(' +', ' ', sentence)
        #title= re.sub('[^0-9a-zA-Z]+ ', '', title)
        print(sentence)
        print(pmID)
        if pmID not in pmDict:
            pmDict[pmID] = OrderedDict()

        pmDict[pmID][sentence] = OrderedDict()
            #pmDict[pmID].append(title.lower())

    if line.startswith('mappings'):
        line = line.replace("mappings(","").replace(").",",")
        firstSplit = line.split("map(")[1:]
        #titleList.append(title.lower())

        for sublines in firstSplit:
            secondSplits = sublines.split("ev(")[1:]
            for parts in secondSplits:
                #pmDict[pmID].append(parts.split(',')[1].replace("\'",""))
                #pmDict[pmID].append(parts.split(',')[-4])

                #CUIList.append(parts.split(',')[1].replace("\'",""))
                #CUIList.append(parts.split(',')[-4])
                """pmDict[pmID][title].append(parts.split(',')[1].replace("\'",""))
                pmDict[pmID][title].append(parts.split(',')[-4])"""
                cuid = parts.split(',')[1].replace("\'","")
                offset = parts.split(',')[-4]
                cuiName = parts.split(',')[2].replace("\'","").lower()
                cuiName = re.sub('[^0-9a-zA-Z]+ ', '', cuiName)
                if cuiName not in list(pmDict[pmID][sentence].keys()):
                    pmDict[pmID][sentence][cuiName] = []
                    pmDict[pmID][sentence][cuiName].append(cuid)
                    pmDict[pmID][sentence][cuiName].append(offset)





    return pmDict

file = sc.textFile('/media/gaurav/Elements/Thesis/data/MMO/prd/test3.txt')
sc.parallelize([],1).count()
pmDict = OrderedDict()
dd = sc.accumulator(pmDict,accum_param=OrderedDict())

#print(file.collect())
dicti = file.map(lambda x : _parseFile(x,dd))

print(dicti.collect())


