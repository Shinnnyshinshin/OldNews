
"""
Script to download Twitter archive files from archive.org and transfer them to S3 bucket
"""
S3Path = "s3://twitterarchive12345/AllArchives/"
TwitterPathFile  = open("FilePaths.sh", 'r')
TwitterPaths = TwitterPathFile.readlines()
BashFileForDownloading = open("BashToDoAllDownloads.sh", "w")
for path_to_dl in TwitterPaths:
    path_to_dl = path_to_dl.strip()
    # parse just the file name
    file_name = path_to_dl.split("/")[-1]
    # download to EC2
    BashFileForDownloading.write( "wget " + path_to_dl + "\n")
    # transfer to S3
    BashFileForDownloading.write( "aws s3 cp \"" + file_name + "\" " + S3Path + "\n")
    # erase from EC2
    BashFileForDownloading.write( "rm " + file_name + "\n")

    
