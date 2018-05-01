import os
from SimpleDicomToolkit import Logger

class FileScanner(Logger):
    
    @staticmethod
    def files_in_folder(dicom_dir, recursive=False):
        """ Find all files in a folder, use recursive if files inside subdirs
        should be included. """

        # Walk through a folder and recursively list all files
        if not recursive:
            files = os.listdir(dicom_dir)
        else:
            files = []
            for root, dirs, filenames in os.walk(dicom_dir):
                for file in filenames:
                    full_file = os.path.join(root, file)
                    if os.path.isfile(full_file):
                        files += [full_file]
            # remove system specific files and the database file that
            # start with '.'
            files = [f for f in files if not os.path.split(f)[1][0] == '.']

        return files
    @staticmethod
    def scan_files(dicom_dir, recursive=False, existing_files = []):
        """ Find all files in folder and add dicom headers to database.
        If overwrite is False, existing files in database will not be updated
        """
        
        # recursively find all files in the folder
        files = FileScanner.files_in_folder(dicom_dir, recursive=recursive)

        # extract relative path
        files = [os.path.relpath(f, dicom_dir) for f in files]

        # normalze path
        files = [os.path.normpath(file) for file in files]   
       
        # use sets for performance
        files = set(files)
        existing_files = set(existing_files)

        # files that are in database but were not found in file folder
        not_found = list(existing_files.difference(files))
       

        # files in path but not in database
        new_files = list(files.difference(existing_files))
       

        return new_files, not_found