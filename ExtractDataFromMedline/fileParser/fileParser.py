import pickle
import xml.etree.ElementTree as ET

import os

import logging
import multiprocessing as mp


logging.basicConfig(level=logging.ERROR)
fileParserlog = logging.getLogger(__name__)
hdlr = logging.FileHandler('../Downloader/log/2017.log')
formatter = logging.Formatter('%(asctime)s %(message)s')
hdlr.setFormatter(formatter)
fileParserlog.addHandler(hdlr)
fileParserlog.setLevel(logging.INFO)

def processFile(filePair):
    fileURL = filePair[0]
    #print(fileURL)
    yr2017 = filePair[1]
    destinationFolder = '../Downloader/processedData/'+str(filePair[2])+'/'

    fileName = fileURL.split('/')[-1].split('.')[0]+'.txt'
    destFileURL = destinationFolder+fileName
    if not os.path.isfile(destFileURL):
        try :
            tree =ET.parse(fileURL) # ET.parse('../Downloader/2012/medline12n0655.xml') #
            root = tree.getroot()
            with open(destFileURL,'w') as fout:

            #fileOut = list()
                if not bool(yr2017):
                    for child in root:


                        try:
                            article = child.find("Article")
                            pubID = child.find("PMID").text
                        #NLMID = child.find("NlmUniqueID").text
                            articleTile = article.find("ArticleTitle").text
                            pubYear = child.find("DateCreated").find("Year").text
                            country = ''
                            country = child.find("MedlineJournalInfo").find("Country")
                            if affliation is not None:
                                country = child.find("MedlineJournalInfo").find("Country").text
                            else :
                                ADD other condition here
                                country = child.find().text

                            affliation = article.find("Affiliation")
                            affliationtext = ''
                            if  affliation is not None:
                                affliationtext = article.find("Affiliation").text.split('.')[0].split(',')[-1]

                            fout.write(str(pubID)+'|'+str(pubYear) + '|' + str(articleTile) + '|' + str(country) +  '|' + str(affliationtext) +'\n')

                        except ValueError:
                            fileParserlog.info('file: '+fileURL+', error:'+str(article)+ValueError)

                else:
                    for child in root:

                        try:

                            pubID = child.find("MedlineCitation").find("PMID").text
                            articleTile = child.find("MedlineCitation").find("Article").find("ArticleTitle").text
                            pubYear = child.find('PubmedData').find('History').find('PubMedPubDate').find('Year').text
                            country = child.find("MedlineCitation").find("MedlineJournalInfo").find("Country").text

                            affliation = child.find("Affiliation")
                            affliationtext = ''
                            if affliation is not None:
                                affliationtext = child.find("Affiliation").text.split('.')[0].split(',')[-1]

                            fout.write(str(pubID) + '|' + str(pubYear) + '|' + str(articleTile) + '|' + str(country) + '|' + str(affliationtext) + '\n')
                        except Exception as e:
                           fileParserlog.info('file: '+fileURL + pubID + ', error:' + str(e))


            fileParserlog.info(' '+fileName+' dumped')
        except Exception as e:

            fileParserlog.error(fileURL+str(e))

    else:
        fileParserlog.info(' ' + fileName + ' exits!!')

def getFolderList(mainFolderURL):
    """
    This functions returns all the subfolders in a particular folder(Downloader/data)
    :param mainFolderURl: url of data folder
    :return: list of all the subfolder in Downloader/data
    """

    return [x[1] for x in (os.walk(mainFolderURL))][0]


def convertUTF8toASCII(yyFolder, mainFolderURL='../Downloader'):
    """
    This function take year, extract all files in the that year and give it to processFile, which saves the processed file in processData folder in Downloader folder
    :param listOfFolders:
    :param mainFolderURL:
    :return:
    """
    fileParserlog.info(str(yyFolder) + ' started')
    listOfFiles = [x[2] for x in (os.walk(mainFolderURL+'/'+str(yyFolder)))][0]

    val = False
    if yyFolder== 2017:
        val = True

    srcfileList = []
    for file in listOfFiles:
        srcfileList.append(mainFolderURL+'/'+str(yyFolder)+'/'+file)

    valList = [val]*len(srcfileList)
    yyList = [yyFolder]*len(srcfileList)

    p = mp.Pool(4)
    p.map(processFile, zip(srcfileList, valList,yyList))

    fileParserlog.info(str(yyFolder)+' Done')
    
    pass


def main():
    #subfolderList = getFolderList(mainFolderURL='../Downloader/data')
    subfolderList = [2017]
    for yy in subfolderList:
        convertUTF8toASCII(yy)



if __name__ == '__main__':
    main()

