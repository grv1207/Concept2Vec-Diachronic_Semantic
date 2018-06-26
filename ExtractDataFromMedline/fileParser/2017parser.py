

import xml.etree.ElementTree as ET
import logging

logging.basicConfig(level=logging.INFO)
fileParserlog = logging.getLogger("fileParser")

def processFile(fileURL):
    fileName = fileURL.split('/')[-1].split('.')[0]+'.txt'
    tree = ET.parse(fileURL) #('../Downloader/2002/medline02n0001.xml')
    root = tree.getroot()
    #with open(destinationFolder+fileName,'w') as fout:


    for child in root:

        #try:
            #article = child.find("ArticleTitle")
            #article = child.find("Article")
            #pubID = child.find("PMID").text
        #NLMID = child.find("NlmUniqueID").text
            #articleTile = article.find("ArticleTitle").text
            #pubYear = child.find("DateCreated").find("Year").text
            #fout.write(str(pubYear) + "_" + str(pubID) + '|' + articleTile+'\n')
        print(child.find("MedlineCitation").find("Article").find("ArticleTitle").text)
        print(child.find("MedlineCitation").find("PMID").text)
        print(child.find('PubmedData').find('History').find('PubMedPubDate').find('Year').text)


        """except:
            fileParserlog.info('file: '+fileURL+', error:'+str(article))
        finally:
            print(str(pubYear) + "_" + str(pubID) + '|' + articleTile)"""

if __name__ == '__main__':
    fileURL = '../Downloader/data/2017/medline17n0001.xml'
    processFile(fileURL=fileURL)
