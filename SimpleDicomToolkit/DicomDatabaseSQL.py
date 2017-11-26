# -*- coding: utf-8 -*-
"""
Created on Tue Sep  5 16:54:20 2017

@author: HeyDude
"""
import os
import json
import dicom



from SimpleDicomToolkit.SQLiteWrapper import SQLiteWrapper, logging
from SimpleDicomToolkit.progress_bar import progress_bar
from SimpleDicomToolkit.read_dicom import DicomReadable
from SimpleDicomToolkit.dicom_parser import DicomFiles, Header, \
dicom_dataset_to_dict, VR_FLOAT, VR_INT

class Database(SQLiteWrapper, DicomReadable):
    """ Creates a Sqlite3 table from a list of dicom files. Each header entry
    is stored in a seperate column. Sequences are stored in a single column """

    FILENAME_COL    = 'dicom_file_name' # colum in table that stores filenames
    TAGNAMES_COL    = 'dicom_tag_names' # column that stores tag names for file
    DATABASE        = 'minidicom.db'    # default file name for database
    MAIN_TABLE      = 'DicomMetaData'   # stores values for each tag

    _chunk_size     = 1000              # number of files to read before committing
    _folder         = None
    LOG_LEVEL       = logging.DEBUG

    def __init__(self, folder=None, rebuild=False, scan=True, silent=False):
        """ Create a dicom database from folder

            rebuild: Deletes the database file and generates a new database
            scan:    Scans for all dicom files in the folder and updates
                     the database. Missing files will be removed as well

        """
        if silent:
            self.LOG_LEVEL = logging.ERROR
        # database file

        database = os.path.join(folder, Database.DATABASE)

        self.folder = os.path.abspath(folder)


        if rebuild and os.path.exists(database):
            os.remove(database)
            scan = True

        super().__init__(table_name=None, database=database)

        self._logger.info('Root folder: %s', self.folder)
        self._logger.info('Databsase file: %s', self.database)
        self.connect()
        self._create_table() # create empty table if not exists

        if scan:
            new_files, not_found = self._scan_files()
            self._update_db(new_files, not_found, silent=silent)
        self.close()

    def _create_table(self):
        cmd = """CREATE TABLE  IF NOT EXISTS {table}
                 (id INTEGER AUTO_INCREMENT PRIMARY KEY,
                  {file_name} TEXT UNIQUE,
                  {tag_names} TEXT)"""

        cmd = cmd.format(table=self.MAIN_TABLE,
                         file_name=self.FILENAME_COL,
                         tag_names=self.TAGNAMES_COL)

        self.execute(cmd, close=False)

    def _scan_files(self):
        """ Find all files in folder and add dicom headers to database.
        If overwrite is False, existing files in database will not be updated
        """
        self._logger.info('Scanning files....')
        # recursively find all files in the folder
        self._logger.info('Populating file list in %s', self.folder)


        files = Database.files_in_folder(self.folder, recursive=True)

        self._logger.debug('%s files in folder', len(str(files)))

        # extract relative path

        files = [os.path.relpath(f, self.folder) for f in files]
        self._logger.debug('path extracted')

        # normalze path
        files = [os.path.normpath(file) for file in files]
        self._logger.debug('path normalized')


        #file names already in database
        files_in_db = self.files
        self._logger.debug('Query file in database')


        # use sets for performance
        files = set(files)
        files_in_db = set(files_in_db)

        # files that are in database but were not found in file folder
        not_found = list(files_in_db.difference(files))
        self._logger.debug('%s files not found', len(not_found))

        # files in path but not in database
        new_files = list(files.difference(files_in_db))
        self._logger.info('%s new files found', len(new_files))

        return new_files, not_found

    def _update_db(self, new_files, not_found, silent=False):

        # handle files that were not found
        for file in not_found:
            self.remove_file(os.path.join(self.folder, file, close=False))

        self.close() # commit removed files

        if not new_files:
            return # nothing to add

        # progress bar
        progress = lambda i: progress_bar(i, len(new_files), prefix='Database',
                                          suffix='Complete')


        if not silent:
            progress(0) # show progress bar

        # divide files into equally sized batches. Changes to the db
        # are committed after each batch
        batches = self._chunks(new_files, self._chunk_size)
        columns = None

        for j, batch in enumerate(batches):
            self.connect() # connect to update db
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

            self._logger.debug('Committing changes to db')
            self.close() # commit changes

    def insert_file(self, file, _existing_column_names=None, close=True):
        """ Insert a dicom file to the database """
        if _existing_column_names is None:
            _existing_column_names = self.column_names(self.MAIN_TABLE,
                                                       close=False)

        # read file from disk
        fullfile = os.path.join(self.folder, file)
        try:
            header = dicom.read_file(fullfile, stop_before_pixels=True)
        except dicom.errors.InvalidDicomError:
            self._logger.info('%s not a dicom file', fullfile)
            return _existing_column_names

        # convert header to dictionary
        hdict = dicom_dataset_to_dict(header)
        hdict[self.TAGNAMES_COL] = list(hdict.keys()) # store tag names
        hdict[self.FILENAME_COL] = file # add filenmae to dictionary

        # determine which columns need to be added to the database
        newcols = [c for c in hdict.keys() if c not in _existing_column_names]

        # add columns
        self._add_column_for_tags(newcols)

        # encode dictionary values to json and stor in database
        self.insert_row_dict(self.MAIN_TABLE, Database._encode(hdict),
                             close=close)
        if close:
            self.close()

        self._logger.debug(newcols)
        return newcols

    def __dir__(self):
        # enable autocomplete dicom tags
        res = dir(type(self)) + list(self.__dict__.keys())
        res += self.tag_names
        return res

    def __getattr__(self, attr):
        # enable dicom tags as attributes (default pydicom behaviour)
        if attr in super().column_names(self.MAIN_TABLE):
            values = self.get_column(attr, parse=True)

            if len(values) == 1:
                return values[0]
        else:
            # print(attr + ' not valid!')
            raise AttributeError
        # self._logger.info(attr + ' ' + str(values))

        return values

    def __len__(self):
        return len(self.files)

    def __str__(self):
        return 'Database with {0} files'.format(len(self))

    def __repr__(self):
        return self.__str__()

    @property
    def files(self):
        """ Retrieve all files with full path from the database """
        return self.get_column(self.FILENAME_COL, close=False)


    @property
    def tag_names(self):
        """ Return the tag names that are in the database """
        tag_names = self.column_names(self.MAIN_TABLE, close=False)
        non_tags_in_db = (self.ID, self.FILENAME_COL)

        for name in non_tags_in_db:
            tag_names.remove(name)

        return  sorted(tag_names)

    def get_column(self, column_name, unique=True,
                   sort=True, close=True, parse=True):
        """ Return the unique values for a column with column_name """
        values = super().get_column(self.MAIN_TABLE, column_name,
                                    sort=sort, close=False)
        if unique:
            values = Database._unique_list(values)

        if parse:
            # use tag names as attributes in dicom sequences
            if values and isinstance(values[0], str):
                values = [json.loads(value) for value in values]

            # values = Header.factory(values)

        if close:
            self.close()
        return values

    def query(self, column_names=None, close=True, sort_by=None,
              partial_match=False, **kwargs):
        """ Query the table """
        if column_names is None:
            # if columns are None return values for every dicom tag
            column_names = self.tag_names

        # always return file names and tag
        if self.FILENAME_COL not in column_names:
            column_names.append(self.FILENAME_COL)

        # always return tag names
        if self.TAGNAMES_COL not in column_names:
            column_names.append(self.TAGNAMES_COL)

        for tag, value in kwargs.items():
            # if type(value) is not str:
            if not partial_match:
                # exact values are json dumps, inexact values
                # work on json strings.
                kwargs[tag] = json.dumps(value)

        # perform query by super class
        columns = super().query(self.MAIN_TABLE, column_names=column_names,
                                close=close, sort_by=sort_by,
                                partial_match=partial_match, **kwargs)

        # format result if any
        if columns:
            result = self._format_response(columns, column_names)
            result.SUV = self.SUV
            return result
        else:
            return columns

    def _format_response(self, columns, column_names):
        # return a dictionary with keys the filenames. Values are dicts
        # comprised of the tag_name and tag_value

        columns = list(zip(*columns))
        columns = [list(col) for col in columns] # convert cols to list


        for col in columns:
            for j, value in enumerate(col):
                if isinstance(value, str):
                    col[j] = json.loads(value)
        # return columns, column_names

        #extract filenames
        files = columns.pop(column_names.index(self.FILENAME_COL))
        files = [os.path.join(self.folder, file) for file in files]

        # construct metadata for each file
        metadata = {}
        for index, file in enumerate(files):
            hdict = {} # tags for file

            for name, col in zip(column_names, columns):
                value = col[index] # select value in column for file

                hdict[name] = value

            # tags that are in this specific file
            tag_names = hdict.pop(self.TAGNAMES_COL)

            # remove tags that are not tags for this file
            hdict = dict([(k, v) for k, v in hdict.items() if k in tag_names])
            # enable indexing for tags including sub indexing sequences
            hdict = Header.from_dict(hdict)
            metadata[file] = hdict

        metadata = DicomFiles(**metadata)
        return metadata

    def remove_file(self, file_name, close=True):
        """ Remove file from database """
        self.delete_rows(self.MAIN_TABLE, column=self.FILENAME_COL,
                         value=file_name, close=False)

        if close:
            self.close()

    def _add_column_for_tags(self, tag_names):
        # add columns to the databse for the given tag_names
        # the sqlite datatype will be determined from the dicom value
        # representation in the datadict of pydicom

        existing_columns = self.column_names(self.MAIN_TABLE, close=False)

        for tag_name in tag_names:
            if tag_name not in existing_columns:
                tag = dicom.datadict.tag_for_name(tag_name)

                if tag is None:
                    self._logger.info(('Error getting pydicom infor for {0}.'.format(tag_name),
                                       ' Assuming values are text'))
                    sqlite_type = self.TEXT
                else:
                    VM = dicom.datadict.dictionaryVM(tag)
                    VR = dicom.datadict.dictionaryVM(tag)

                    if VM == 1:
                        sqlite_type = Database._sqlite_type_for_VR(VR)
                    else:
                        # multiple values are stored as json string
                        sqlite_type = self.TEXT


                self.add_column(self.MAIN_TABLE, tag_name, close=False,
                                var_type=sqlite_type)
    @staticmethod
    def _chunks(iterable, chunksize):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(iterable), chunksize):
            yield iterable[i:i + chunksize]

    @staticmethod
    def _encode(header_dict):
        """ convert to json """
        encoded_dict = {}
        for k, v in header_dict.items():
            if not isinstance(v, (float, int)):
                encoded_dict[k] = json.dumps(v)
            else:
                encoded_dict[k] = v
        return encoded_dict

    @staticmethod
    def _sqlite_type_for_VR(VR):
        # map dicom value representation to sqlite data type
        if VR in VR_FLOAT:
            return Database.REAL
        elif VR in VR_INT:
            return Database.INTEGER
        else:
            return Database.TEXT

    @staticmethod
    def _unique_list(l):
        # Get Unique items in list while preserving ordering of elements
        seen = set()
        seen_add = seen.add

        unique_list = [x for x in l if not (str(x) in seen or seen_add(str(x)))]

        return unique_list



if __name__ == "__main__":
    test_folder = '/Users/marcel/Documents/Horos Data/Database.noindex'
    db = Database(folder = test_folder, silent = True)
