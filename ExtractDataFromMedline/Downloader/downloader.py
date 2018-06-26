import os
import subprocess

def _downloader(folderName, pageURL, fileList):
    """
    Creates a folder with year name and then downloads file in it
    :param pageURL : URL of the file
   :param folderName: folder where file is stored
   :return:
   """
    if not os.path.isdir(folderName):
        os.mkdir(str("./" + folderName))
        #subprocess.call(['MMO-Data'])
        #os.subprocess.call(['mkdir processData'])
    for fileno in fileList:
        fileName = 'text.out_' + fileno + '.gz'
        fileURL = pageURL +fileName
        #subprocess.call(['./downloader.sh', fileURL , fileno, str("./" + folderName)])

        subprocess.call(['curl ' +fileURL+'  > '+fileno])


if __name__=='__main__':

    _downloader('2015','https://mbr.nlm.nih.gov/Download/MetaMapped_Medline/2015/',[str(x).zfill(2) for x in range(1,151)])