import sys
import os

path = "/lustre/test"
fname = []
data = ''
current_user = sys.argv[1]
directory = '/lustre/test/'+current_user
for root,d_names,f_names in os.walk(path):
        for f in f_names:
                fname.append(os.path.join(root, f))
                with open(os.path.join(root,f),'r') as file:
                        data +=file.read()



print("fname = %s" %fname)
print("data "+data)
print("directory "+directory)
print("current_user "+current_user)

fil = open(directory + "/"+current_user+"_file.txt","w+")
fil.write(data)
fil.close
