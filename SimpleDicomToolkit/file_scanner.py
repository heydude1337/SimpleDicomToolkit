import os
from SimpleDicomToolkit import Logger

class FileScanner(Logger):
    
    @staticmethod
    def files_in_folder(folder, recursive=False, absolute_path=False):
        """ Find all files in a folder, use recursive if files inside subdirs
        should be included. """

        # Walk through a folder and recursively list all files
        if not recursive:
            files = [os.path.join(folder, file) for file in os.listdir(folder)\
                     if os.path.isfile(file)]
        else:
            files = []
            for root, dirs, filenames in os.walk(folder):
                for file in filenames:
                    full_file = os.path.join(root, file)
                    if os.path.isfile(full_file):
                        files += [full_file]
            # remove system specific files and the database file that
            # start with '.'
            files = [f for f in files if not os.path.split(f)[1][0] == '.']
        
        # extract relative path
        if absolute_path:
            files = [os.path.abspath(f) for f in files]
        else:
            files = [os.path.relpath(f, folder) for f in files]

        # normalze path
        files = [os.path.normpath(file) for file in files]   

        return files

    @staticmethod
    def compare(files, existing_files):
        # use sets for performance
        files = set(files)
        existing_files = set(existing_files)

        # files that are in database but were not found in file folder
        not_found = list(existing_files.difference(files))
       

        # files in path but not in database
        new_files = list(files.difference(existing_files))
       

        return new_files, not_found