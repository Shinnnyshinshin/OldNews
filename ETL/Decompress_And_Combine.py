import os
import sys
def decomp_tar_files(file_list):
    with open(file) as f:
        for file in f:
            file = file.rstrip("\n")
            os.system("sudo mkdir /home/ubuntu/S3/BZ2/{}".format(file[:-4]))
            os.system("sudo tar -xvf /home/ubuntu/S3/{} -C /home/ubuntu/S3/BZ2/{}/".format(file,file[:-4]))

def copy_bz2_to_central_folder(path_for_bz2):
    count = 0
    for dirpath, dirs, files in os.walk(my_path):
      for filename in files:
        fname = os.path.join(dirpath,filename)
        if fname.endswith('.bz2'):
            new_name = fname[20:].replace("/", "_")
            os.system("sudo mv " + fname + " " + "/home/ubuntu/S3/Unzipped/" + new_name)

if __name__ == '__main__':
    decomp_tar_files(file_list = "file_list.txt")
    copy_bz2_to_one_folder(my_path = '/home/ubuntu/S3Alias/')
