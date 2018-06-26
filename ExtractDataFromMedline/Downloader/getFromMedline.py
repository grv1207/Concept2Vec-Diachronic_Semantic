"""
This script is getting the the .xml.gz from medline server(https://mbr.nlm.nih.gov/Download/Baselines/).
Medline server has many folder under with year name and each folder has alot of files of type .xml.gz.
This script uses CURL command to simple download those files and save them in respective folder(year name).
"""
import os

"""
author : Gaurav Vashisth , Date : 27 April 2018
"""
from bs4 import BeautifulSoup
import requests
from urllib.request import urlopen
import subprocess
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Downloader")
import multiprocessing as mp

class Downloader():
    def __init__(self, mainURL,afterLink):
        """
        :param mainURL: we get main url
        we extract years(folder) name from getYear() and then for each year we extract file in that year(folder) and no of
        file are equal to noOfFiles
        """
        self.mainURL = mainURL
        self.yearList = self._getYear(mainURL,afterLink)
        #for yy in self.yearList:
        yy = self.yearList
        #yy = mainURL.split('/')[-1]
        logger.info("download started for year" + str(yy))
        filList= []
        for filetag in [ff for ff in self._getFiles(yy)]:
            destFileURL = './'+str(yy) + '.'.join(filetag['href'].split('.')[:2])    #<a href="medline14n0001.xml.gz">medline14n0001.xml.gz</a>
            if not os.path.isfile(destFileURL):

                filList.append(filetag['href'])
            else :
                print(destFileURL,'exists')

        yyList = [yy]*len(filList)
        pageURLList = [self.mainURL+yy] * len(filList)
        p = mp.Pool(2)
        p.map(self._downloader, zip(filList, yyList, pageURLList))
        #self._downloader(fileName = file,folderName=yy,pageURL=)

        logger.info("download complete for year"+str(yy))

    def contentFolder(self, pageURL):
        """
        :param pageURL: url of specific directory
        :return: all href<a> available in a particular folder(url)
        """

        page = urlopen(pageURL)
        content = BeautifulSoup(page, "html.parser")
        return content('a')

    def _getYear(self, url, afterLink):
        """
       :param url: page url
       :param Content: content on the url
       :return: list of years on in the content
       """

        completeContent = self.contentFolder(url)

        hrefList = completeContent[5:]  # remove first 4 links(parent director...)
        yearList = []

        for yrs in hrefList:
            yearList.append(yrs['href'])

        return yearList[-1]    #-1:-3

    def _getFiles(self, year):
        """
        :param: year folder name
        :return: list of all the files in that year folder
        """
        allContent = self.contentFolder(pageURL=self.mainURL+str(year))

        return allContent[5:]   # remove first 4 links(parent director...)



    def _downloader(self, tripleList):
        """
        Creates a folder with year name and then downloads file in it
        :param fileName : file name
        :param pageURL : URL of the file
       :param folderName:
       :return:
       """
        fileName, folderName, pageURL = tripleList[0],tripleList[1],tripleList[2]

        if not os.path.isdir(folderName):
            os.mkdir(str("./" + folderName))

            #subprocess.call(['mkdir processData'])
        subprocess.call(['./downloader.sh',pageURL+fileName,fileName,folderName])




if __name__ == "__main__":
    #url = "https://mbr.nlm.nih.gov/Download/Baselines/"
    url = "https://mbr.nlm.nih.gov/Download/Baselines/"
    down = Downloader(mainURL=url,afterLink=1)
