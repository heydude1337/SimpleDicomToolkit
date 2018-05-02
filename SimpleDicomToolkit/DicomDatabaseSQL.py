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
from SimpleDicomToolkit import Logger, FileScanner
from SimpleDicomToolkit.SQLiteWrapper import SQLiteWrapper
from SimpleDicomToolkit.progress_bar import progress_bar
from SimpleDicomToolkit.dicom_reader import DicomReadable
from SimpleDicomToolkit.dicom_parser import Parser, Header 


class Database(DicomReadable, Logger):

    """ Creates a Sqlite3 table from a list of dicom files. Each header entry
    is stored in a seperate column. Sequences are stored in a single column """

    _FILENAME_COL    = 'dicom_file_name' # colum in table that stores filenames
    _TAGNAMES_COL    = 'dicom_tag_names' # column that stores tag names for file
    _DATABASE_FILE   = 'minidicom.db'    # default file name for database
    _MAIN_TABLE      = 'DicomMetaDataTable'   # stores values for each tag
    _FILENAME_TABLE  = 'FileNameTable' # stores non dicom files
    _QUERY_RESULT    = 'QueryResultTable' # stores queries
    
    _chunk_size     = 1000  # number of files to read before committing
    _folder         = None
    _LOG_LEVEL       = logging.INFO

    def __init__(self, folder=None, rebuild=False, scan=True, silent=False, 
        SUV = True, in_memory, file_list = None):
        """ Create a dicom database from folder

            rebuild: Deletes the database file and generates a new database
            scan:    Scans for all dicom files in the folder and updates
                     the database. Missing files will be removed as well
            silent:  Supress progressbar and log messages except errors

        """
        self.SUV = SUV
        self.__active_table = None
        self.database_file = None
        
        if silent:
            self._LOG_LEVEL = logging.ERROR
        if in_memory:
            self.database_file = SQLiteWrapper.IN_MEMORY
        elif folder is not None:
            self.folder = os.path.abspath(folder) # None returns cwd
            self.database_file = os.path.join(self.folder, 
                                              Database._DATABASE_FILE)
        
        
        if rebuild and not in_memory and os.path.exists(self.database_file):
            os.remove(self.database_file)
            scan = True
        if self.database_file is not None:
            self._init_database()
            if scan:
                self._update_db(file_list=file_list, silent=silent)
    
    @property
    def files(self):
        """ Retrieve all dicom files with  path from the database """
        return self.database.get_column(self._active_table, 
                                        self._FILENAME_COL, 
                                        sort=False,
                                        close=True)
    @property
    def _files(self):
        """ Retrieve all files with path from the database """
        dicom_files = self.files
        non_dicom_files = self.database.get_column(self._FILENAME_TABLE, 
                                        self._FILENAME_COL,
                                        sort=False,
                                        close=True)
        return [*dicom_files, *non_dicom_files]
    
    @property
    def columns(self):
        return self.database.column_names(self._active_table, close=True)
   
    @property
    def tag_names(self):
        """ Return the tag names that are in the database """
     
        non_tags_in_db = (self.database.ID, self._FILENAME_COL,
                          self._TAGNAMES_COL)

        return  sorted([tagname for tagname in self.columns\
                        if tagname not in non_tags_in_db])
    
    @property
    def _active_table(self):
        # the table from which data is retrieved
        if self.__active_table is None:
            self.__active_table = self._MAIN_TABLE
        return self.__active_table
    
    @_active_table.setter
    def _active_table(self, table_name):
        self.__active_table = table_name
    
    @property
    def headers(self):
        """ Get pydicom headers from values in database. Pydicom headers 
        are generated from database content. """
        if len(self.files) > self.MAX_FILES:
            msg = 'Number of files exceeds MAX_FILES property'
            raise IOError(msg)
        
        if self._headers is not None:
            return self._headers
        
        if len(self) < 1:
            self._headers = []
            return self.headers
        
        headers = []
        uids = self.SOPInstanceUID
        if not isinstance(uids, list):
            uids = [uids]
       
        headers = [self.header_for_uid(uid) for uid in uids]
        
        if len(headers) == 1:
            headers = headers[0]
            
        self._headers = headers
                
        return self._headers
    @property
    def header(self):
        headers = self.headers
        if len(headers) == 0:
            return None
        elif len(headers) > 1:
            raise IndexError('Mulitple Instances Found')
        else:
            return headers[0]
        
    def header_for_uid(self, sopinstanceuid):
        sopinstanceuid = Parser.encode_value_with_tagname('SOPInstanceUID',
                                                          sopinstanceuid)
        h_dicts = self.database.get_row_dict(self._active_table, 
                                             SOPInstanceUID=sopinstanceuid)
        if len(h_dicts) == 0:
            msg = 'SOPInstanceUID {0} not in database'
            self.logger.info(msg.format(sopinstanceuid))
        elif len(h_dicts) > 1:
            msg = 'SOPInstanceUID {0} not unique'
            raise ValueError(msg.format(sopinstanceuid))
        h_dict = h_dicts[0]
        [h_dict.pop(key) for key in (self._FILENAME_COL, self._TAGNAMES_COL)]
        return self._decode(h_dict)
        
    def reset(self):
        """ After a query a subset of the database is visible, use reset
        to make all data visible again. """
        self._active_table = self._MAIN_TABLE
        self._headers = None
        super().reset()
        return self
        
    def _init_database(self):
        # make connection to the database and make columns if they don't exist
        self.database = SQLiteWrapper(database_file=self.database_file)
        self.database._LOG_LEVEL = self._LOG_LEVEL
        self.logger.info('Root folder: %s', self.folder)
        self.logger.info('Databsase file: %s', self.database.database_file)
        self.database.connect()
        self._create_main_table() # create empty table if not exists
        self._create_file_table()
        self._headers = None
        self.database.close()

    def _create_file_table(self):
        # create the table that holds non dicom files
        cmd = """CREATE TABLE  IF NOT EXISTS {table}
                 (id INTEGER AUTO_INCREMENT PRIMARY KEY,
                  {file_name} TEXT UNIQUE)
                 """

        cmd = cmd.format(table=self._FILENAME_TABLE,
                         file_name=self._FILENAME_COL,
                         tag_names=self._TAGNAMES_COL)

        self.database.execute(cmd)
        
    def _create_main_table(self):
        # create the main table with dicom tags as columns
        cmd = """CREATE TABLE  IF NOT EXISTS {table}
                 (id INTEGER AUTO_INCREMENT PRIMARY KEY,
                  {file_name} TEXT UNIQUE,
                  {tag_names} TEXT)"""

        cmd = cmd.format(table=self._MAIN_TABLE,
                         file_name=self._FILENAME_COL,
                         tag_names=self._TAGNAMES_COL)

        self.database.execute(cmd)

   

    def _update_db(self, silent=False, new_files=None):
        # scan for file new and removed files in the folder. Update the
        # database with new files, remove files that are no longer in folder
        if not new_files:
            new_files, not_found = FileScanner.scan_files(self.folder, 
                recursive=True, existing_files=self._files)
        else:
            not_found = []
            
        # handle files that were not found
       
        self.remove_files(not_found)

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
        columns = self.columns
     
        for j, batch in enumerate(batches):
            self.database.connect() # connect to update db
           
            for i, file in enumerate(batch):
                # display progress
                if not silent:
                    progress(j * self._chunk_size + i + 1)

                # insert file and keep track of newly create columns without
                # additional database queries
                try:
                    new_columns = self.insert_file(file,
                                _existing_column_names=columns, close=False)
                                               
                except dicom.errors.InvalidDicomError:
                    # list file as non dicom
                    self.database.insert_list(self._FILENAME_TABLE, file,
                                      column_names = self._FILENAME_COL,
                                      close = False)
                    
                    continue
                
                if columns is not None:
                    columns = list(set(columns + new_columns))
                else:
                    columns = new_columns
                
                
            self.logger.debug('Committing changes to db')
            self.database.close() # commit changes

    def insert_file(self, file, _existing_column_names=None, close=True):
        """ Insert a dicom file to the database """
        if _existing_column_names is None:
            _existing_column_names = self.database.column_names(self._MAIN_TABLE,
                                                       close=False)

        # read file from disk
        fullfile = os.path.join(self.folder, file)
     
        header = dicom.read_file(fullfile, stop_before_pixels=True)


        # convert header to dictionary
        hdict = self._encode(header)
        hdict[self._TAGNAMES_COL] = json.dumps(list(hdict.keys())) # store tag names
        hdict[self._FILENAME_COL] = file # add filenmae to dictionary

        # determine which columns need to be added to the database
        newcols = [c for c in hdict.keys() if c not in _existing_column_names]

        # add columns
        self._add_column_for_tags(newcols, skip_check=True)

        # encode dictionary values to json and stor in database
        try:
            self.database.insert_row_dict(self._MAIN_TABLE, hdict,
                                 close=close)
        except:
            msg = ('Could not insert file: {0}'.format(file))
            raise IOError(msg)

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
        if attr in self.database.column_names(self._active_table):
            values = self.get_column(attr, parse=True)

            if len(values) == 1:
                return values[0]
        else:
            raise AttributeError(attr)
   

        return values

    def __len__(self):
        return len(self.files)

    def __str__(self):
        return 'Database with {0} files'.format(len(self))

    def __repr__(self):
        return self.__str__()

    def get_column(self, column_name, distinct=True,
                   sort=False, close=True, parse=True):
        """ Return the unique values for a column with column_name """
        values = self.database.get_column(self._active_table, column_name,
                                    sort=sort, distinct=distinct, close=False)

        if parse:
            values = [Parser.decode_entry(column_name, vi)[0] for vi in values]

        if close:
            self.database.close()
        return values

    def query(self, close=True, sort_by=None,
              partial_match=False, sort_decimal=False, **kwargs):
        """ Query the table """
        column_names = self.database.column_names(self._active_table)

        for tag, value in kwargs.items():
            if not isinstance(value, str):
                value = str(value)

            if not partial_match:
                # exact values are json dumps, inexact values
                # work on json strings.
                kwargs[tag] = json.dumps(value)

        self.database.query(self._active_table, column_names=column_names,
                                close=close, sort_by=sort_by, 
                                sort_decimal=sort_decimal,
                                partial_match=partial_match,
                                destination_table=self._QUERY_RESULT, **kwargs)
        
        self._active_table = self._QUERY_RESULT
        
        self._clean()
        return self

    def _clean(self):
        # remove dicom tag columns when the tag is not present in any of 
        # the files in the active table
        valid_columns_str = self.database.get_column(self._active_table, 
                                                     self._TAGNAMES_COL,
                                                     distinct=True)
        valid_columns = [json.loads(col) for col in valid_columns_str]
        valid_columns = set(itertools.chain(*valid_columns))
        valid_columns = [*valid_columns, self._TAGNAMES_COL, self._FILENAME_COL]
        
        
        active_columns = set(self.database.column_names(self._active_table))
        
        obsolete_columns = active_columns.difference(valid_columns)
        self.logger.debug('Removing columns: ' + str(obsolete_columns))
        for column in obsolete_columns:
            self.database.delete_column(self._active_table, column)
        
        return obsolete_columns
        
    def _encode(self, header):
        # pydicom header to dictionary with (json) encoded values
        return Header.from_pydicom_header(header)
    def _decode(self, hdict):
        return Parser.decode(hdict)
    
    def remove_files(self, file_names):
        """ Remove file list from the database """
        for file_name in file_names:
            self.remove_file(file_name, close=False, clean = False)
        
        self._clean()
        
        self.database.close()
        
    def remove_file(self, file_name, close=True, clean = True):
        """ Remove file from database """
        if self.__active_table is not self._MAIN_TABLE:
            msg = ('Removing files cannot be done after query, please use '
                   'reset() to undo query selection.')
            raise RuntimeError(msg)
        self.database.delete_rows(self._MAIN_TABLE, column=self._FILENAME_COL,
                         value=file_name, close=False)
        
        self.database.delete_rows(self._FILENAME_TABLE, column=self._FILENAME_COL,
                         value=file_name, close=False)
        if clean:
            self._clean()
            
        if close:
            self.database.close()
        
    def _add_column_for_tags(self, tag_names, skip_check = False):
        # add columns to the databse for the given tag_names
        # the sqlite datatype will be determined from the dicom value
        # representation in the datadict of pydicom
        if not skip_check:
            existing_columns = self.database.column_names(self._MAIN_TABLE, 
                                                          close=False)
        else:
            existing_columns = []

        for tag_name in tag_names:
            if tag_name not in existing_columns:
                self.database.add_column(self._MAIN_TABLE, tag_name, 
                                close=False, var_type=self.database.TEXT)
    
    def _count_tag(self, tagname):
        if not hasattr(self, tagname):
            count = 0
        values = getattr(self, tagname)
        if not isinstance(values, (list, tuple)):
            count = 1
        else:
            count = len(values)
        return count
   
    @property
    def series_count(self):
        return self._count_tag('SeriesInstanceUID')
    @property
    def study_count(self):
        return self._count_tag('StudyInstanceUID')
    @property
    def patient_count(self):
        return self._count_tag('PatientID')
    
    @staticmethod
    def _chunks(iterable, chunksize):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(iterable), chunksize):
            yield iterable[i:i + chunksize]

 
if __name__ == "__main__":
    pass
#    database = Database(folder = 'C:/Users/757021/Data/Orthanc', rebuild=False, scan=False)
#    print(len(database))
#    database.remove_file(database.files[0])
#    print(len(database))
#    database._update_db()
#    print(len(database))