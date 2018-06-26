"""
This is script is used map both the sources and generate file for each year and in each file store all the pmID(all data corresponding to it)
, CUI Mapping, sentences having ConceptName concatenated
"""
import pickle
from collections import OrderedDict
import commonFunctions as cf
import re

import sys
import pyprog


PATTERN = re.compile('([^\s\w]|_)+')

mylogger = cf.MMOLogger().getLogger(__name__,'./log/mapper.txt')



def batchFileReaderSRC1(pathURL,batchSize=100):
    """
    :param pathURL:
    :return: a list of PMID's in source1 and default length of list is 1000
    """
    pmIDlist = []
    with open(pathURL, 'r') as fin:
        for line in fin:
            #if i ==batchSize:
            #    break
            #else:
                #lineList = line.split('|')
            pmIDlist.append(line.strip('\n'))

    return pmIDlist


def dictionaryOffoundPMID(pmIDList, pathURL='/media/gaurav/Elements/Thesis/data/MMO/CompleteData/files/processed/'):
    """
    :param pmIDList:
    :param pathURL:
    :return: A dictionary containing all the pmID found in the pmIDList, with key as pmID
    """
    mylogger.info('finding the values for all the PMID in pmIDList')
    prog = pyprog.ProgressBar("", " Done", len(pmIDList),complete_symbol="█", not_complete_symbol="-")
    prog.update()
    totale_element = len(pmIDList)
    count = 0
    foundPMIDDict = dict()

    for dicts in cf.getAllfiles(pathURL):
        #bar.next()
        if (len(foundPMIDDict) != len(pmIDList)):
            try:
                #bar.next()
                with open(pathURL + dicts, 'rb') as fin:
                    dictipickle = pickle.load(fin)
                    for ids in pmIDList:
                        if ids in dictipickle.keys():
                            count = count+1
                            #print('%d : percent done',int(totale_element/count))
                            prog.set_stat(count)
                            prog.update()
                            #print('\n')
                            foundPMIDDict[ids] = dictipickle[ids]

            except Exception as e :
                mylogger.error('dictionaryOffoundPMID| dicts:  %s| %s' % (dicts, e))

        else:
            prog.end()
            break

    return foundPMIDDict


def yearDictionarySRC1(pathURL, batchSize=100):
    """
    :param pathURL:
    :return: a Orderded dictionary containing year as key and other attributes as value
    e.g
    OrderedDict([('1865',
              [['16691646', 'United States', ''],
               ['16691647', 'United States', ''],
               ['16691648', 'United States', '']]),
             ('1866',......
    """
    yearDict = OrderedDict()
    listOflines = batchFileReaderSRC1(pathURL)
    for line in listOflines:
        try:
            linel = line.split('|')
            yy = linel[1]
            pm = linel[0]
            country = linel[3]
            aff = linel[4]
            if yy not in yearDict:
                yearDict[yy] = []

            yearDict[yy].append([pm, country, aff])
        except Exception as e :
            mylogger.error('yearDictionarySRC1| line %s| %s'% (linel,e))
    return yearDict


def mapSentenceWithCUI(sentence, ConceptValues, newsentence):
    """
    :param sentence:  original sentence
    :param values: conceptName and ConceptID
    :param newsentence: modified sentence
    :return: modified sentence mapped with new CUID
    this func take original sentence and newsentence and based upon offset values in ConceptValues it maps CUIID(fromConceptValues)
     in newsentence(which is return)
    """
    conceptID = ConceptValues[0]
    # offList = values[1]
    worCount = 0
    wordOff = []
    try:

        if (len(ConceptValues[1]) > 1):
            """this is the case when there is a list of several offset for single Conceptname
            """

            for offValues in ConceptValues[1]:
                wordOff.append(offValues[0])
                worCount = offValues[1]
        else:
            """this is the case when there one offset for single Conceptname
            """
            wordOff.append(ConceptValues[1][0][0])
            worCount = ConceptValues[1][0][1]

    except Exception as e:
        mylogger.error('CUIMapping| Sentence-ConceptValues %s, %s | %s' % (sentence,ConceptValues, e))
    word2replace = sentence[wordOff[0]:wordOff[-1] + worCount]
    #if(word2replace in )

    return newsentence.replace(' '+sentence[wordOff[0]:wordOff[-1]+' ' + worCount], conceptID)


def CUIMapping(dictValue,pmID):
    """
    :param dictValue: values for a PMID from SRC2
    :return:   for given PMID ,sentence ID  and all CUI mapped sentences
    example : for completeDict['16691646']
    (OrderedDict([('ti.1 | Statement of Cases of Gonorrhoeal and Purulent Ophthalmia treated in the       Desmarres (U. S. Army) Eye and Ear Hospital, Chicago, Illinois, with Special       Report of Treatment Employed.',
               OrderedDict([('Statement', ['C1710187', [(0, 9)]]),
                            ('Cases', ['C0868928', [(13, 5)]]),
                            ('GONORRHEA', ['C0018081', [(22, 11)]]),
                            ('treatment_nos', ['C0001554', [(172, 9)]]),
                            ('Employed', ['C0557351', [(182, 8)]])]))]),
 'C1710187 of C0868928 of C0018081 and C0259800 C1522326 in the       desmarres (C0041703. C0680778) C0015392 and C0013443 C0019994, C0008044, C0020898, C0332287 C0205555       C0684224 of C0001554 C0557351.')
    """
    newSentenceList = []
    if(pmID =='16691654'):
        print(pmID)
    for setencekey in dictValue:  # completeDict['16691646']
        try:

            setenceID = setencekey.split('|')[0].strip(' ')
            sentence = setencekey.split('|')[1].strip(' ').lower()
            newsentence = sentence
            for conceptName, values in dictValue[setencekey].items():
                newsentence = mapSentenceWithCUI(sentence, values, newsentence)

            #use sentenceID along here

            #modifiedSentence = PATTERN.sub('', newsentence)
            modifiedSentence = re.sub('\W+', ' ', newsentence)
            newSentenceList.append(str(pmID)+'|'+setenceID+'| '+modifiedSentence)

        except Exception as e:
            mylogger.error('CUIMapping| pmID %s| %s' % (pmID, e))

    return  newSentenceList


def CUIMappingOffset(dictValue, pmID):
    """
    :param dictValue: values for a PMID from SRC2
    :return:   for given PMID ,sentence ID  and all CUI mapped sentences
    example : for completeDict['16691646']
    (OrderedDict([('ti.1 | Statement of Cases of Gonorrhoeal and Purulent Ophthalmia treated in the       Desmarres (U. S. Army) Eye and Ear Hospital, Chicago, Illinois, with Special       Report of Treatment Employed.',
               OrderedDict([('Statement', ['C1710187', [(0, 9)]]),
                            ('Cases', ['C0868928', [(13, 5)]]),
                            ('GONORRHEA', ['C0018081', [(22, 11)]]),
                            ('treatment_nos', ['C0001554', [(172, 9)]]),
                            ('Employed', ['C0557351', [(182, 8)]])]))]),
 'C1710187 of C0868928 of C0018081 and C0259800 C1522326 in the       desmarres (C0041703. C0680778) C0015392 and C0013443 C0019994, C0008044, C0020898, C0332287 C0205555       C0684224 of C0001554 C0557351.')
    """
    newSentenceList = []
    if (pmID == '17756924'):
        print(pmID)
    for setencekey in dictValue:  # completeDict['16691646']
        try:

            setenceID = setencekey.split('|')[0].strip(' ')
            sentence = setencekey.split('|')[1].strip(' ').lower()
            # newsentence = sentence
            """for conceptName, values in dictValue[setencekey].items():
                newsentence = mapSentenceWithCUI(sentence, values, newsentence)"""
            ConceptAttributes = dictValue[setencekey]
            varX = list(ConceptAttributes.items())
            sortedConceptAttributes = sortingConceptValues(varX)

            newsentence, noOFConceptADD, noOfConcetIgnored = mapSentenceWithOffset(sentence, sortedConceptAttributes,pmID)

            modifiedSentence = re.sub('\W+', ' ', newsentence)
            #newSentenceList.append(str(pmID) + '|' + setenceID + '| ' + modifiedSentence+' |'+noOFConceptADD+'|'+noOfConcetIgnored)
            newSentenceList.append((
            str(pmID), setenceID, modifiedSentence, noOFConceptADD, noOfConcetIgnored))

        except Exception as e:
            mylogger.error('CUIMappingOffset| pmID %s| %s' % (pmID, e))

    return newSentenceList


def sortingConceptValues(ConceptValueList):
    """
    return a sorted ConceptValueList based upon last tuple offset values
    """

    return sorted(ConceptValueList, key=lambda k: k[1][1][-1][0] + k[1][1][-1][1])


def mapSentenceWithOffset(sentence, sortedConceptValues,pmID):
    """
    takes and sentence and a list of sortedConceptValues, and it starts replace sentence from back for each value in sortedvalue

    """
    #checkOffset = []
    last_added_Offset =len(sentence)
    nbr_added = 0
    nbr_discarded = 0
    try:

        for pairValues in sortedConceptValues[::-1]:  # reverse Order
            CUID = pairValues[1][0]  # get CUID(C1720176) from  this tuple ('On', ['C1720176', [(0, 2)]])
            offSetValues = pairValues[1][1]  # (U_S ['C0041703', [(90, 1), (93, 1)]])
            # if len(offSetValues)>1:          #[(90, 1), (93, 1)]
            startOffset = int(offSetValues[0][0])  # 90
            endOffset = offSetValues[-1][0]  # 93
            lastWordLen = offSetValues[-1][1]  # 1
            # nl[:162] +'C0684224' +nl[162+6:]
            #if (startOffset, lastWordLen) not in checkOffset:
            if startOffset < last_added_Offset and endOffset + lastWordLen < last_added_Offset:

                sentence = sentence[:startOffset] + CUID + sentence[endOffset + lastWordLen:]
                last_added_Offset = startOffset
                nbr_added += 1
            else:
                nbr_discarded +=1
                #checkOffset.append((startOffset, lastWordLen))
    except Exception as e:
        mylogger.error('mapSentenceWithOffset| pmID %s,pairValues %s| %s' % (pmID,pairValues, e))

    return sentence, nbr_added,nbr_discarded


def generateDictionaryForYEAR(yearDictfromSRC1, pmIDDictfromSRC2):
    """
    Generate a file for each year and store all PMID's mapped CUI along with sentence ID in the taht file
    :param yearDict:
    :return:
    """
    yrCUIdict = OrderedDict()
    for k in yearDictfromSRC1.keys():
        # for each year in yearDictfromSRC1
        for v in yearDictfromSRC1[k]:
            # for each value(pmID,country,affliation) in yearDictfromSRC1[k]
            try:
                if k not in yrCUIdict:
                    yrCUIdict[k] = []

                yrCUIdict[k].append(CUIMappingOffset(dictValue=pmIDDictfromSRC2[v[0]],pmID= v[0]))

            #print(v)

            except Exception as e:
                mylogger.error('generateDictionaryForYEAR| k %s| %s' % (k, e))


    return yrCUIdict

def saveContent(contentPerDictionaryKey,filename, destinationURL='/media/gaurav/Elements/Thesis/data/mappedData/'):
    """if not cf.filepathExist(destinationURL+filename):
           'create a file at give URL'
           cf.createFile(filename,destinationURL)
       'append in this file' """

    with open(destinationURL+filename,'a') as fout:
        for line in contentPerDictionaryKey:
            for values in line:
                fout.write('|'.join(map(str, values)))
                #fout.write(' ')
                fout.write('\n')


def main(folderURL):
    size = 50000

    listOfFiles = cf.getAllfiles(folderURL)
    for filename in listOfFiles:
        url = folderURL+filename
        yrDict = yearDictionarySRC1(pathURL=url)
        mylogger.info('dictionary containing year as key created and has %d unique key(s)',len(yrDict))


        lineList = batchFileReaderSRC1(pathURL=url)  #'url of complete file sorted'

        pmdIDList = [x.split('|')[0] for x in lineList]
        mylogger.info('pmdIDList generated with length %d', len(pmdIDList))

        destURL = '/media/gaurav/Elements/Thesis/src/ExtractDataFromMedline/Downloader/all/sorted/bin/'
        pmIDDictfromSRC2 = dictionaryOffoundPMID(pmdIDList)

        cf.saveDictionary(fileObject=pmIDDictfromSRC2,filepath=destURL+'pmIDDictfromSRC2.bin')
        mylogger.info('pmIDDictfromSRC2.bin dumped ')

        newyrDict = generateDictionaryForYEAR(yrDict, pmIDDictfromSRC2)
        destURL = '/media/gaurav/Elements/Thesis/src/ExtractDataFromMedline/Downloader/all/sorted/bin/'
        cf.saveDictionary(fileObject=newyrDict, filepath=destURL+'newyrDict.bin')
        mylogger.info('newyrDict.bin dumped ')

        for k,v in newyrDict.items():
            saveContent(contentPerDictionaryKey= v, filename=k)
            mylogger.info('file generated for year %s ',k)


if __name__ == '__main__':
    url ='/media/gaurav/Elements/Thesis/src/ExtractDataFromMedline/Downloader/all/sorted/'

    main(url)
