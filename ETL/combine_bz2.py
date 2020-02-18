import os
def copy_bz2_to_one_folder(my_path):
    count = 0
    for dirpath, dirs, files in os.walk(my_path):
      for filename in files:
        fname = os.path.join(dirpath,filename)
        if fname.endswith('.bz2'):
            new_name = fname[25:].replace("/", "_")
            count += 1
            os.system("sudo mv " + fname + " " + "/home/ubuntu/S3/Unzipped/" + new_name)
            if count % 1000 == 0:
                print("Number of files moved:", count)

if __name__ == '__main__':
    copy_bz2_to_one_folder(my_path = '/home/ubuntu/S3Alias/')
