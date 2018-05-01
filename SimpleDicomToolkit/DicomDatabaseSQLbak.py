# -*- coding: utf-8 -*-
"""
Created on Tue Sep  5 16:54:20 2017

@author: HeyDude
"""
import os
import json

try:
    import pydicom as dicom
except ImportError:
    import dicom

import logging
import itertools
from SimpleDicomToolkit import Logger
from SimpleDicomToolkit.SQLiteWrapper import SQLiteWrapper
from SimpleDicomToolkit.progress_bar import progress_bar
from SimpleDicomToolkit.read_dicom import DicomReadable
from SimpleDicomToolkit.dicom_parser import Parser, Header # DicomFiles, Header, \
#Parser, VR_FLOAT, VR_INT



class Database(DicomReadable, Logger):

    """ Creates a Sqlite3 table from a list of dicom files. Each header entry
    is stored in a seperate column. Sequences are stored in a single column """

    FILENAME_COL    = 'dicom_file_name' # colum in table that stores filenames
    TAGNAMES_COL    = 'dicom_tag_names' # column that stores tag names for file
    DATABASE        = 'minidicom.db'    # default file name for database
    MAIN_TABLE      = 'DicomMetaDataTable'   # stores values for each tag
    QUERY_RESULT    = 'QueryResultTable'
    _chunk_size     = 1000              # number of files to read before committing
    _folder         = None
    _LOG_LEVEL       = logging.ERROR

    def __init__(self, folder=None, rebuild=False, scan=True, silent=False, 
        SUV = True):
        """ Create a dicom database from folder

            rebuild: Deletes the database file and generates a new database
            scan:    Scans for all dicom files in the folder and updates
                     the database. Missing files will be removed as well

        """
        self.SUV = SUV
        self._active_table = None
        
        if silent:
            self.LOG_LEVEL = logging.ERROR
            
        self.database_file = os.path.join(folder, Database.DATABASE)

        self.folder = os.path.abspath(folder)

        if rebuild and os.path.exists(self.database_file):
            os.remove(self.database_file)
            scan = True
        
        if self.folder is not None:
            self._init_database()
        
        if scan:
            self._update_db()
    @property
    def files(self):
        """ Retrieve all files with full path from the database """
        return self.database.get_column(self.active_table, self.FILENAME_COL, close=False)


    @property
    def tag_names(self):
        """ Return the tag names that are in the database """
        tag_names = self.database.column_names(self.active_table, close=False)
        non_tags_in_db = (self.database.ID, self.FILENAME_COL)

        for name in non_tags_in_db:
            tag_names.remove(name)

        return  sorted(tag_names)
    @property
    def active_table(self):
        if self._active_table is None:
            self._active_table = self.MAIN_TABLE
        return self._active_table
    
    @active_table.setter
    def active_table(self, table_name):
        self._active_table = table_name
    
    def reset(self):
        self.active_table = self.MAIN_TABLE
        
    def _init_database(self):
        self.database = SQLiteWrapper(database_file=self.database_file)

        self.logger.info('Root folder: %s', self.folder)
        self.logger.info('Databsase file: %s', self.database.database_file)
        self.database.connect()
        self._create_table() # create empty table if not exists

        self.database.close()

    def _create_table(self):
        cmd = """CREATE TABLE  IF NOT EXISTS {table}
                 (id INTEGER AUTO_INCREMENT PRIMARY KEY,
                  {file_name} TEXT UNIQUE,
                  {tag_names} TEXT)"""

        cmd = cmd.format(table=self.MAIN_TABLE,
                         file_name=self.FILENAME_COL,
                         tag_names=self.TAGNAMES_COL)

        self.database.execute(cmd, close=False)

   

    def _update_db(self, silent=False):

        new_files, not_found = FileScanner.scan_files(self.folder, 
                                                      recursive=True,
                                                      existing_files=self.files)
        # handle files that were not found
        for file in not_found:
            self.remove_file(file, close=False)

        self.database.close() # commit removed files

        if not new_files:
            return # nothing to add

        # progress bar
        progress = lambda i: progress_bar(i, len(new_files), prefix='Database',
                                          suffix='Complete', length=79)


        if not silent:
            progress(0) # show progress bar

        # divide files into equally sized batches. Changes to the db
        # are committed after each batch
        batches = self._chunks(new_files, self._chunk_size)
        columns = None

        for j, batch in enumerate(batches):
            self.database.connect() # connect to update db
            for i, file in enumerate(batch):
                # display progress
                if not silent:
                    progress(j * self._chunk_size + i + 1)

                # insert file and keep track of newly create columns without
                # additional database queries
                new_columns = self.insert_file(file,
                                               _existing_column_names=columns,
                                               close=False)
                if columns is not None:
                    columns = list(set(columns + new_columns))
                else:
                    columns = new_columns

            self.logger.debug('Committing changes to db')
            self.database.close() # commit changes

    def insert_file(self, file, _existing_column_names=None, close=True):
        """ Insert a dicom file to the database """
        if _existing_column_names is None:
            _existing_column_names = self.database.column_names(self.MAIN_TABLE,
                                                       close=False)

        # read file from disk
        fullfile = os.path.join(self.folder, file)
        try:
            header = dicom.read_file(fullfile, stop_before_pixels=True)
        except dicom.errors.InvalidDicomError:
            self.logger.info('%s not a dicom file', fullfile)
            return _existing_column_names

        # convert header to dictionary
        hdict = self._encode(header)
        hdict[self.TAGNAMES_COL] = json.dumps(list(hdict.keys())) # store tag names
        hdict[self.FILENAME_COL] = file # add filenmae to dictionary

        # determine which columns need to be added to the database
        newcols = [c for c in hdict.keys() if c not in _existing_column_names]

        # add columns
        self._add_column_for_tags(newcols)

        # encode dictionary values to json and stor in database
        try:
            self.database.insert_row_dict(self.MAIN_TABLE, hdict,
                                 close=close)
        except:
            print('Could not insert file: {0}'.format(file))
            raise

        if close:
            self.database.close()

        self.logger.debug(newcols)
        return newcols

    def __dir__(self):
        # enable autocomplete dicom tags
        res = dir(type(self)) + list(self.__dict__.keys())
        res += self.tag_names
        return res

    def __getattr__(self, attr):
        # enable dicom tags as attributes (default pydicom behaviour)
        if attr in self.database.column_names(self.MAIN_TABLE):
            values = self.get_column(attr, parse=True)

            if len(values) == 1:
                return values[0]
        else:
            # print(attr + ' not valid!')
            raise AttributeError(attr)
        # self.logger.info(attr + ' ' + str(values))

        return values

    def __len__(self):
        return len(self.files)

    def __str__(self):
        return 'Database with {0} files'.format(len(self))

    def __repr__(self):
        return self.__str__()

    

    def get_column(self, column_name, distinct=True,
                   sort=True, close=True, parse=True):
        """ Return the unique values for a column with column_name """
        values = self.database.get_column(self.active_table, column_name,
                                    sort=sort, distinct=distinct, close=False)

        if parse:
            values = [Parser.decode_entry(column_name, vi) for vi in values]

        if close:
            self.database.close()
        return values

    def query(self, close=True, sort_by=None,
              partial_match=False, **kwargs):
        """ Query the table """
        column_names = self.database.column_names(self.active_table)

        for tag, value in kwargs.items():
            if not isinstance(value, str):
                value = str(value)

            if not partial_match:
                # exact values are json dumps, inexact values
                # work on json strings.
                kwargs[tag] = json.dumps(value)

        self.database.query(self.active_table, column_names=column_names,
                                close=close, sort_by=sort_by,
                                partial_match=partial_match,
                                new_table_name=self.QUERY_RESULT, **kwargs)
        
        self.active_table = self.QUERY_RESULT
        return self

    def _clean(self):
        valid_columns_str = self.database.get_column(self.active_table, 
                                                     self.TAGNAMES_COL,
                                                     distinct=True)
        valid_columns = [json.loads(col) for col in valid_columns_str]
        valid_columns = set(itertools.chain(*valid_columns))
        
        active_columns = set(self.database.column_names(self.active_table))
        
        obsolete_columns = active_columns.intersection(valid_columns)
        
        return obsolete_columns
        
    def _encode(self, header):
        return Header.from_pydicom_header(header)
    
    def remove_file(self, file_name, close=True, clean = True):
        """ Remove file from database """
        self.database.delete_rows(self.MAIN_TABLE, column=self.FILENAME_COL,
                         value=json.dumps(file_name), close=False)

        if clean:
            self._clean
            
        if close:
            self.database.close()
        
    def _add_column_for_tags(self, tag_names):
        # add columns to the databse for the given tag_names
        # the sqlite datatype will be determined from the dicom value
        # representation in the datadict of pydicom

        existing_columns = self.database.column_names(self.MAIN_TABLE, close=False)

        for tag_name in tag_names:
            if tag_name not in existing_columns:
                self.database.add_column(self.MAIN_TABLE, tag_name, 
                                close=False, var_type=self.database.TEXT)
                                
    @staticmethod
    def _chunks(iterable, chunksize):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(iterable), chunksize):
            yield iterable[i:i + chunksize]

#  

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
       

        #file names already in database
       
        # use sets for performance
        files = set(files)
        existing_files = set(existing_files)

        # files that are in database but were not found in file folder
        not_found = list(existing_files.difference(files))
       

        # files in path but not in database
        new_files = list(files.difference(existing_files))
       

        return new_files, not_found
    
if __name__ == "__main__":
    database = Database(folder = 'C:/Users/757021/Data/Orthanc', rebuild=False)