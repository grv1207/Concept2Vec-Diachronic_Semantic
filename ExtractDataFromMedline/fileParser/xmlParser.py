"""
This Parser parses the xml file that we get from mm_print framework
Purpose: to extract PMID as key and ConceptID and  ConceptName in list

"""
import pickle
import argparse

"""
author : Gaurav Vashisth , Date : 04 Mai 2018
"""


import xml.etree.ElementTree as ET
from lxml import etree
import logging

logging.basicConfig(level=logging.INFO)
xmlParser = logging.getLogger("xmlParser")

class ParseXML():
    """
    This gets XMLS file from mm_print and returns as pickle with same name of input file
    the pickle has a PMID as key and lists of ConceptID and ConceptName in two separate lists
    """
    def __init__(self, xmlFile):
        self.xmlFile = xmlFile
        self.pmIDdict = dict()

    def getDictionary(self):
        """
        This function reads multiple root xml tags present in a file and parse each root tag to extractElement()
        :return:
        """
        accumulated_xml = []
        with open(self.xmlFile,'r') as fread:
            while True:
                line = fread.readline()

                if line.startswith('<?xml'):
                    accumulated_xml = []
                else:
                    accumulated_xml.append(line)
                    if line.__contains__('</MMOs>'):
                        PMID,ConceptID = self.extractElement(accumulated_xml)
                        self.pmIDdict[PMID] = ConceptID

    def dictLength(self):
        with open(self.xmlFile+'.bin','wb') as fout:
            pickle.dump(self.pmIDdict,fout,pickle.HIGHEST_PROTOCOL)
        return len(self.pmIDdict)

    def extractElement(self, rootList):
        #tree = etree.fromstring(''.join(rootList))
        #utterance = tree.find("Utterances")

        #ConceptID = [x.replace('<CandidateCUI>','').replace('</CandidateCUI>','').strip('\n').strip(' ')
                     #for x in rootList if "CandidateCUI" in x ]
        ConceptID = []

        for x in rootList:
            if "CandidateCUI" in x:
                ConceptID.append(x.replace('<CandidateCUI>','').replace('</CandidateCUI>','').strip('\n').strip(' '))
            elif "PMID" in x:
                PMID = x.replace('<PMID>','').replace('</PMID>','').strip('\n').strip(' ')

        #print(str(PMID)+':'+str(set(ConceptID)))

        return PMID,ConceptID



if __name__=="__main__":

     parser = argparse.ArgumentParser(description='Process some integers.')
     parser.add_argument('--inFile', type=str,
                         help='xml file to parse')
     args = parser.parse_args()

     prxml = ParseXML(args.inFile)
     prxml.getDictionary()
     print(prxml.dictLength())

