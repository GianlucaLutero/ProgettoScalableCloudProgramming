import os
import sys


def replace(folder_path, old, new):
    for path, subdirs, files in os.walk(folder_path):
    	print("Directory list")
    	print(path)
    	print(subdirs)
    	print(files)

    	for name in files:
        	print("Renaming...")
        	print(name)
        	if(name.lower() != "_success"):
        		file_path = os.path.join(path,name)
        		tmp = os.path.dirname(file_path)
        		new_name = os.path.join(path,name.lower().replace(name,os.path.basename(tmp)))
        		print("Nuovo nome")
        		print(tmp)
        		print(new_name)
        		os.rename(file_path, new_name)


def main():
	print("OOOOOOOOOOOOOOOOO")
	root = "C:\\Users\\Gianluca\\Desktop\\result"
	replace(root,'cluster','part')


if __name__ == "__main__":
	main()