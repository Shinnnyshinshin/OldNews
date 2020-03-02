import os
import sys

class Decompress_And_Combine:
    """
    Function to loop through each file in file_list (which has been downloaded to S3) and untar them.
    """
    def decomp_tar_files(self, file_list):
        with open(file_list) as f:
            for file in f:
                file = file.rstrip("\n")
                os.system("sudo mkdir /home/ubuntu/S3/BZ2/{}".format(file[:-4]))
                os.system("sudo tar -xvf /home/ubuntu/S3/{} -C /home/ubuntu/S3/BZ2/{}/".format(file,file[:-4]))

    """
    Function to walk through the folder with all BZ2 Files and move them to a central "unzipped" folder. 
    """
    def copy_bz2_to_central_folder(self, path_for_bz2):
        for dirpath, dirs, files in os.walk(path_for_bz2):
            for filename in files:
                fname = os.path.join(dirpath,filename)
            if fname.endswith('.bz2'):
                new_name = fname[20:].replace("/", "_")
                os.system("sudo mv " + fname + " " + "/home/ubuntu/S3/Unzipped/" + new_name)

if __name__ == '__main__':
    Decompress_And_Combine().decomp_tar_files(file_list="file_list.txt")
    Decompress_And_Combine().copy_bz2_to_central_folder(path_for_bz2='/home/ubuntu/S3Alias/')



