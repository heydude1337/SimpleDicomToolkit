# -*- coding: utf-8 -*-
"""
Created on Tue Sep  5 16:54:20 2017

@author: HeyDude
"""
import os
import json
import warnings
import logging
import pydicom


import SimpleITK as sitk
import SimpleDicomToolkit as sdtk


VERSION = 0.93

class Database(sdtk.Logger):

    """ Creates a Sqlite3 table from a list of dicom files. Each header entry
    is stored in a seperate column. Sequences are stored in a single column """

    _path           = None
    _DATABASE_FILE  = 'minidicom.db'    # default file name for database
    _images         = None # cache dict with images
    _image          = None # cache single image
    _headers        = None # cache list of headers
    _tagnames       = None # cache for tagnames in current selection
    _MAX_FILES       = 5000 # max number of files to be read by property images
    _sort_slices_by  = None # Dicom field name to sort slices by field value
    
    def __init__(self, path, force_rebuild=False, scan=True, silent=False,
                 SUV=True, in_memory=False, use_private_tags=False):
        """ 
        Create a dicom database from path

        force_rebuild: Deletes the database file and generates a new database
        scan:          Scans for all dicom files in the path and updates
                       the database. Missing files will be removed as well
        silent:        Supress progressbar and log messages except errors
        in_memory:     Don't save database to disk. Creates a temporary 
                       database in memory.
        use_private_tags: Set to True to include private tags in the database.
                          [Experimental]
            

        """
        if silent:
            self._LOG_LEVEL = logging.ERROR
            
        
        super().__init__()
        
        

        self.builder = DatabaseBuilder(path=path, scan=scan,
                                       force_rebuild=force_rebuild,
                                       in_memory=in_memory,
                                       use_private_tags=use_private_tags,
                                       silent=silent)

        self.logger.info('Database building completed')

        self.database = self.builder.database

        self.SUV = SUV
        self._selection = {}


        self.reset()
        self.database.close()

    def __dir__(self):
        # enable autocomplete dicom tags
        res = dir(type(self)) + list(self.__dict__.keys())
        res += self.tag_names
        return res

    def __getattr__(self, attr):
        # enable dicom tags as attributes (default pydicom behaviour)
        if attr in self.tag_names:
            values = self.get_column(attr, parse=True)

            if len(values) == 1:
                return values[0]
        else:
            raise AttributeError(attr)

        return values

    def __len__(self):
        return len(self.files)

    def __str__(self):
        msg = ('{header}\n'
               '{selection}'
               '\t- {npatients} patient(s)\n'
               '\t- {nstudies} studie(s)\n'
               '\t- {nseries} serie(s)\n'
               '\t- {ninstances} instance(s)\n'
               '\t{mbytes}MB ')

        selection = ''
        if self._selection:
            header = 'Selection from database:'
            for key, value in self.selection.items():
                selection += '\t{key}: {value}\n'.format(key=key, value=value)
        else:
            header = 'Database contents:'

        mbytes = self.database.sum_column(self.builder.MAIN_TABLE,
                                          self.builder.FILE_SIZE_COL,
                                          **self._selection)
        mbytes = 0 if mbytes is None else mbytes/1e6

        msg = msg.format(header=header,
                         selection=selection,
                         npatients=self.patient_count,
                         nstudies=self.study_count,
                         nseries=self.series_count,
                         ninstances=self.instance_count,
                         mbytes=round(mbytes))

        return msg

    def __repr__(self):
        return self.__str__()

    @property
    def selection(self):
        # decode values for presentation
        selection = {}
        for key, value in self._selection.items():
            if key in self.non_tag_columns:
                selection[key] = value
            else:
                selection[key] = sdtk.Decoder.decode_entry(key, value)[0]
        return selection

    @property
    def files(self):
        """ Retrieve all files with  path from the database """
        file_list = self.get_column(self.builder.FILENAME_COL, close=True)
        file_list = [file.replace('\\', '/') for file in file_list]
        return file_list

    @property
    def files_with_path(self):
        path = self.builder.path
        file_list = [os.path.join(path, file) for file in self.files]
        return file_list

    @property
    def sorted_files_with_path(self):
        join = lambda file: os.path.join(self.builder.path, file)
        return [join(file) for file in self.sorted_files]

    @property
    def columns(self):
        """ Return all column names in database. """
        return self.database.column_names(self.builder.MAIN_TABLE, close=True)

    @property
    def non_tag_columns(self):
        """ Return column names that are not dicom tagnames """
        return (self.builder.FILENAME_COL,
                self.builder.FILE_SIZE_COL,
                sdtk.SQLiteWrapper.ROWID,
                self.builder.TAGNAMES_COL)

    @property
    def tag_names(self):
        """ Return the tag names that are in the database """
        if self._tagnames is None:
            self._tagnames = self._get_tagnames() # use caching
        return self._tagnames

    @property
    def headers(self):
        """ Get pydicom headers from values in database. Pydicom headers
        are generated from database content. """
        if len(self.files) > self._MAX_FILES:
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

        self._headers = headers

        return self._headers

    @property
    def series_count(self):
        """ Return number of series in database """
        return self._count_tag('SeriesInstanceUID')

    @property
    def study_count(self):
        """ Return number of studies in database """
        return self._count_tag('StudyInstanceUID')

    @property
    def patient_count(self):
        """ Return number of patients in database """
        return self._count_tag('PatientID')

    @property
    def instance_count(self):
        """ Return number of instances in database, equal to number of files"""
        return self._count_tag('SOPInstanceUID')

    @property
    def image(self):
        """ Returns an sitk image for the files in the files property.
            All files must belong to the same dicom series
            (same SeriesInstanceUID). """

        if self._image is not None:
            return self._image

        assert hasattr(self, 'SeriesInstanceUID')
        assert isinstance(self.SeriesInstanceUID, str)

        image = sdtk.dicom_reader.read_serie(self.sorted_files, SUV=False,
                                             folder=self.builder.path)

        # get first uid from file
        uid = self.SOPInstanceUID
        if isinstance(uid, list):
            uid = uid[0]

        # generate header with SUV metadata
        header = self.header_for_uid(uid)

        # calculate suv scale factor
        try:
            bqml_to_suv = sdtk.dicom_reader.suv_scale_factor(header)
        except:
            if self.SUV:
                warnings.warn('\nNo SUV information found, disabling SUV\n',
                              RuntimeWarning)
                bqml_to_suv = 1

        if self.SUV and bqml_to_suv != 1:
            image *= bqml_to_suv
            image.bqml_to_suv = bqml_to_suv
        self._image = image
        return self._image

    @property
    def images(self):
        """ Returns a dictionary with keys the SeriesInstanceUID and
            values the sitkimage belonging tot the set of files belonging to
            the same dicom series (same SeriesInstanceUID). Number of files
            in the files property cannot exceed the MAX_FILES property.
            This prevents reading of too large data sets """

        if len(self.files) > self._MAX_FILES:
            raise IOError('Number of files exceeds MAX_FILES property')

        if self._images is not None:
            return self._images

        assert hasattr(self, sdtk.SERIESINSTANCEUID)

        images = {}
        selection = self.selection.copy()
        for uid in self.SeriesInstanceUID:
            images[uid] = self.select(SeriesInstanceUID=uid).image
            self.reset().select(**selection)

        self._images = images
        return self._images

    @property
    def array(self):
        """ Return dicom data as numpy array """
        return sitk.GetArrayFromImage(self.image)

    @property
    def arrays(self):
        """ Return dicom data as dictionary with key the SeriesInstanceUID
        and value to corresponding numpy array. """
        return dict([(key, sitk.GetArrayFromImage(image)) \
                     for key, image in self.images.items()])

    @property
    def sort_slices_by(self):
        if self._sort_slices_by is None:
            if hasattr(self, 'SliceLocation'):
                self._sort_slices_by = 'SliceLocation'
            elif hasattr(self, 'InstanceNumber'):
                self._sort_slices_by = 'InstanceNumber'
        return self._sort_slices_by

    @sort_slices_by.setter
    def sort_slices_by(self, value):
        """
        Sort slices by given dicom filed
        """
        self._sort_slices_by = value

    @property
    def sorted_files(self):
        """
        Sort files by the dicom tag name stored in property sort_slices_by.
        SimpleIKT Image Reader (unfortunately) expects sorted files to
        create a volume e.g. CT slices.
        """
        sort_by = self.sort_slices_by
        if self.instance_count > 1 and sort_by is None:
            warnings.warn('\nSlice Sorting Failed Before Reading!\n',
                           RuntimeWarning)

        files = self.database.get_column_where(self.builder.MAIN_TABLE,
                                               self.builder.FILENAME_COL,
                                               sort_by=sort_by,
                                               sort_decimal=True,
                                               **self._selection)

        files = [file.replace('\\', '/') for file in files]

        return files



    def select(self, close=True, **kwargs):
        """ Make an selection in the database, based on values of tags
        for example. For example to select only patient 123456 from the
        database:

        database.select(PatientID='123456')

        To select patient 123456 and study 'MyCTScan' do:

        database.select(PatientID='123456').select(StudyDescription='MyCTScan')

        or

        database.select(PatientID='123456', StudyDescription='MyCTScan')

        The latter would use fewer SQL statements, results are the same.

        """

        # encode key word arguments
        for tag, value in kwargs.items():
            if tag in self.non_tag_columns:
                continue

            value = self._encode_value(tag, value)
            kwargs[tag] = value

        self._selection.update(kwargs)
        self._reset_cache()

        return self

    def header_for_uid(self, sopinstanceuid):
        """ Return a pydicom header for the requested sopinstanceuid """
        uid = sdtk.Encoder.encode_value_with_tagname('SOPInstanceUID',
                                                     sopinstanceuid)

        h_dicts = self.database.get_row_dict(self.builder.MAIN_TABLE,
                                             SOPInstanceUID=uid)
        if not h_dicts:
            msg = 'SOPInstanceUID %s not in database'
            self.logger.info(msg, uid)
        elif len(h_dicts) > 1:
            msg = 'SOPInstanceUID {0} not unique'
            raise ValueError(msg.format(uid))
        h_dict = h_dicts[0]
        h_dict = {tag: h_dict[tag] for tag in self.tag_names}

        return self._decode(h_dict)

    def reset(self, tags=None):
        """ After a query a subset of the database is visible, use reset
        to make all data visible again. """

        if tags:
            tags = [tags] if not isinstance(tags, list) else tags
            for tag in tags:
                self._selection.pop(tag, None)
        else:
            self._selection = {}

        self._reset_cache()

        return self

    def get_column(self, column_name, distinct=True,
                   sort=True, close=True, parse=True):
        """ Return the unique values for a column with column_name """
        if sort:
            sort_by = column_name
        else:
            sort_by = None

        values = self.database.get_column(self.builder.MAIN_TABLE,
                                          column_name, sort_by=sort_by,
                                          distinct=distinct,
                                          close=False, **self._selection)

        self.logger.debug('parising column....')

        if parse and column_name not in self.non_tag_columns:
            values = [sdtk.Decoder.decode_entry(column_name, vi)[0] \
                      for vi in values]

        if close:
            self.database.close()
        return values

    def query(self, *args, **kwargs):
        warnings.warn('\nUse select instead of query\n', DeprecationWarning)
        return self.select(*args, **kwargs)

    def _get_tagnames(self):
        """ Return the tag names that are in the database """
        
        tagname_rows = self.get_column(self.builder.TAGNAMES_COL,
                                       distinct=True, parse=False)
        tagnames = set()
        for row in tagname_rows:
            for tagname in json.loads(row):
                tagnames.add(tagname)

        return tuple(tagnames)

    def _reset_cache(self):
        # Clear stored values of this object
        self._headers = None
        self._images = None
        self._image = None
        self._tagnames = None


    @staticmethod
    def _encode_value(tagname, value):
        _, VR, VM = sdtk.Decoder.decode_tagname(tagname)
        if sdtk.SQLiteWrapper.is_between_dict(value):
            for key, v in value.items():
                value[key] = sdtk.Encoder.convert_value(v, VR=VR, VM=VM)
        else:
            value = sdtk.Encoder.convert_value(value, VR=VR, VM=VM)
        return value

    @staticmethod
    def _decode(hdict):
        return sdtk.Decoder.decode(hdict)

    def _count_tag(self, tagname):
        if tagname not in self.tag_names:
            count = 0
        else:
            count = self.database.count_column(self.builder.MAIN_TABLE,
                                               tagname, distinct=True,
                                               **self._selection)
        return count


class DatabaseBuilder(sdtk.Logger):
    """ Build a dicom database from a folder or set of files """
    FILENAME_COL    = 'dicom_file_name' # colum in table that stores filenames
    FILE_SIZE_COL   = 'file_size_bytes' # store size of files
    TAGNAMES_COL    = 'dicom_tag_names' # column that stores tag names for file

    MAIN_TABLE      = 'DicomMetaDataTable'   # stores values for each tag
    _INFO_TABLE      = 'Info'                 # store database version
    _INFO_DESCRIPTION_COL = 'Description'
    _INFO_PATH_COL = 'path'
    _INFO_VALUE_COL = 'Value'
    _FILENAME_TABLE  = 'FileNameTable' # stores non dicom files

    _chunk_size     = 1000  # number of files to read before committing

    def __init__(self, path=None, scan=True, silent=False, database_file=None,
                 force_rebuild=False, in_memory=False, use_private_tags=False):
        if silent:
            self._LOG_LEVEL = logging.ERROR
            
        
        super().__init__()
        
        
        
        self.use_private_tags = use_private_tags
        
        path, file = self._parse_path(path)

        if file is None:
            file = self._get_database_file(path, in_memory=in_memory)


        self.database_file = file

        self.database = self.open_database(database_file=file,
                                           force_rebuild=force_rebuild,
                                           path=path)

        files = self.file_list(self.path, index=scan)

        self._update_db(files=files, silent=silent)
        
        self.path = path
        
        self.database.close()

    @property
    def files(self):
        """ Return all files dicom and non dicom that were added or tried to
        add to the database. These files will not be re-added."""
        return self.database.get_column(self._FILENAME_TABLE,
                                        self.FILENAME_COL)
    @property
    def path(self):
        p = self.database.get_column(self._INFO_TABLE, self._INFO_PATH_COL)[0]
        return p

    @path.setter
    def path(self, path):
        if path != self.path:
            self.database.delete_table(self._INFO_TABLE)
            self._create_info_table(self.database, version=self.version,
                                    path=path)
    @property
    def version(self):
        return DatabaseBuilder.get_version(self.database)


    @version.setter
    def version(self, version):
        if version != self.version:
            self.database.delete_table(self._INFO_TABLE)
            self._create_info_table(self.database, version=version,
                                    path=self.path)

    def open_database(self, database_file, path, force_rebuild=False):
        """ Open the sqlite database in the file, rebuild if asked """
        
        database = sdtk.SQLiteWrapper(database_file)
        database._LOG_LEVEL = self._LOG_LEVEL
        
        

        is_latest = self.get_version(database) ==  VERSION

        self.logger.debug('Databae Version: %s', self.get_version(database))
        self.logger.debug('Latest Version: %s', str(VERSION))
        
        if not is_latest:
            msg = 'Old Database Structure Found, rebuilding recommended!'
            self.logger.info(msg)
        
        if force_rebuild:
            msg = 'Removing tables from: %s'
            self.logger.info(msg, database.database_file)
            database.delete_all_tables()

        if not self.MAIN_TABLE in database.table_names:
            self._create_main_table(database)
        if not self._INFO_TABLE in database.table_names:
            self._create_info_table(database, path=path)
        if not self._FILENAME_TABLE in database.table_names:
            self._create_filename_table(database)
        return database

    @staticmethod
    def get_version(database):
        """ Return the version of the database """
        if DatabaseBuilder._INFO_TABLE not in database.table_names:
            v = 0
        else:
            v = database.get_column(DatabaseBuilder._INFO_TABLE,
                                    DatabaseBuilder._INFO_VALUE_COL)[0]

        return float(v)

    def _get_database_file(self, path, in_memory=False):
        # database file name
        if in_memory:
            database_file = sdtk.SQLiteWrapper.IN_MEMORY

        elif isinstance(path, str) and os.path.isdir(path):
            database_file = os.path.join(path, Database._DATABASE_FILE)
        else:
            path = os.getcwd()
            database_file = os.path.join(path, Database._DATABASE_FILE)
        self.logger.info('Database file %s', database_file)
        return database_file

    def _parse_path(self, path):
        # format the path string
        if isinstance(path, str) and os.path.isdir(path):
            # folder passed
            path = os.path.abspath(path)
            file = None
        elif isinstance(path, str) and os.path.isfile(path):
            file = path
            path = None

        return path, file

    def file_list(self, path, index=True):
        """ Search path recursively and return a list of all files """
        # gather file list
        if index:
            self.logger.info('Scanning for new files')
            files = sdtk.FileScanner.files_in_folder(path, recursive=True)
        else:
            files = []
        return files

    def insert_file(self, file, _existing_column_names=None, close=True):
        """ Insert a dicom file to the database """
        self.logger.debug('Inserting: %s', file)
        self.database.insert_row_dict(self._FILENAME_TABLE,
                                      {self.FILENAME_COL: file})

        if _existing_column_names is None:
            table = DatabaseBuilder.MAIN_TABLE
            _existing_column_names = self.database.column_names(table)

        # read file from disk
        fullfile = os.path.join(self.path, file)

        try:
            header = pydicom.read_file(fullfile, stop_before_pixels=True)
        except FileNotFoundError:
            # skip file when file had been removed between scanning and
            # the time point the file is opened.
            self.logger.debug('{0} not found.'.format(fullfile))
            return _existing_column_names

        # convert header to dictionary
        try:
            hdict = DatabaseBuilder._encode(
                    header, use_private_tags=self.use_private_tags)
        except:
            self.logger.info('Cannot add: %s', file)
            return _existing_column_names

        # store tag names
        hdict[self.TAGNAMES_COL] = json.dumps(list(hdict.keys()))
        hdict[self.FILENAME_COL] = file # add filenmae to dictionary
        hdict[self.FILE_SIZE_COL] = os.path.getsize(fullfile)

        # determine which columns need to be added to the database
        newcols = [c for c in hdict.keys() if c not in _existing_column_names]

        # add columns
        self._add_column_for_tags(newcols, skip_check=True)

        # encode dictionary values to json and stor in database
        try:
            self.database.insert_row_dict(self.MAIN_TABLE, hdict, close=close)
        except:
            msg = ('Could not insert file: {0}'.format(file))
            self.database.close()
            raise IOError(msg)

        if close:
            self.database.close()

        self.logger.debug(newcols)
        self.logger.debug('Inserted: %s', file)
        return newcols

    def remove_files(self, file_names):
        """ Remove file list from the database """
        for file_name in file_names:
            self.remove_file(file_name, close=False)

        self.database.close()

    def remove_file(self, file_name, close=True):
        """ Remove file from database """

        self.database.delete_rows(DatabaseBuilder.MAIN_TABLE,
                                  column=DatabaseBuilder.FILENAME_COL,
                                  value=file_name, close=False)

        self.database.delete_rows(DatabaseBuilder._FILENAME_TABLE,
                                  column=DatabaseBuilder.FILENAME_COL,
                                  value=file_name, close=False)

        if close:
            self.database.close()


    def _add_column_for_tags(self, tag_names, skip_check=False):
        # add columns to the databse for the given tag_names
        # the sqlite datatype will be determined from the dicom value
        # representation in the datadict of pydicom
        if not skip_check:
            existing_columns = self.database.column_names(self.MAIN_TABLE,
                                                          close=False)
        else:
            existing_columns = []

        var_type = sdtk.SQLiteWrapper.TEXT # everything is stored as text

        for tag_name in tag_names:
            if tag_name not in existing_columns:
                self.database.add_column(self.MAIN_TABLE, tag_name,
                                         close=False, var_type=var_type)

    def _update_db(self, files=None, existing_files=None, silent=False):
        # scan for file new and removed files in the path. Update the
        # database with new files, remove files that are no longer in path

        if not files:
            return

        if existing_files is None:
            existing_files = self.files

        new_files, not_found = sdtk.FileScanner.compare(files, existing_files)
        self.logger.info('Adding %i files and removing %i files',
                         len(new_files), len(not_found))

        # handle files that were not found

        self.remove_files(not_found)

        self.database.close() # commit removed files

        if not new_files:
            return # nothing to add

        # progress bar
        pbar = sdtk.progress_bar
        progress = lambda i: pbar(i, len(new_files), prefix='Database',
                                  suffix='Complete', length=79)


        if not silent:
            progress(0) # show progress bar

        # divide files into equally sized batches. Changes to the db
        # are committed after each batch
        batches = self.chunks(new_files, self._chunk_size)
        columns = self.database.column_names(self.MAIN_TABLE)

        for j, batch in enumerate(batches):
            self.database.connect() # connect to update db

            for i, file in enumerate(batch):
                # display progress
                if not silent:
                    progress(j * self._chunk_size + i + 1)

                # insert file and keep track of newly create columns without
                # additional database queries

                try:
                    new_columns = self.insert_file(
                            file,_existing_column_names=columns, close=False
                            )

                except pydicom.errors.InvalidDicomError:
                    continue

                if columns is not None:
                    columns = list(set(columns + new_columns))
                else:
                    columns = new_columns


            self.logger.debug('Committing changes to db')
            self.database.close() # commit changes

    @staticmethod
    def _create_filename_table(database):
        # create the main table with dicom tags as columns
        cmd = """CREATE TABLE  IF NOT EXISTS {table}
                 (id INTEGER AUTO_INCREMENT PRIMARY KEY ,
                  {file_name} TEXT UNIQUE) """


        cmd = cmd.format(table=DatabaseBuilder._FILENAME_TABLE,
                         file_name=DatabaseBuilder.FILENAME_COL)

        database.execute(cmd)

    @staticmethod
    def _create_main_table(database):
        # create the main table with dicom tags as columns
        cmd = """CREATE TABLE  IF NOT EXISTS {table}
                 (id INTEGER AUTO_INCREMENT PRIMARY KEY ,
                  {file_name} TEXT UNIQUE,
                  {file_size} INTEGER,
                  {tag_names} TEXT)"""


        cmd = cmd.format(table=DatabaseBuilder.MAIN_TABLE,
                         file_name=DatabaseBuilder.FILENAME_COL,
                         file_size=DatabaseBuilder.FILE_SIZE_COL,
                         tag_names=DatabaseBuilder.TAGNAMES_COL)

        database.execute(cmd)

    @staticmethod
    def _create_info_table(database, version=VERSION, path=None):
        database.logger.info('Create INFO Table with version: ' + str(version))
        
        cmd = """CREATE TABLE  IF NOT EXISTS {table}
                 (id INTEGER AUTO_INCREMENT PRIMARY KEY,
                  {info_descr} TEXT,
                  {info_val} TEXT,
                  {path_col} TEXT) """

        cmd = cmd.format(table=DatabaseBuilder._INFO_TABLE,
                         info_descr=DatabaseBuilder._INFO_DESCRIPTION_COL,
                         info_val=DatabaseBuilder._INFO_VALUE_COL,
                         path_col=DatabaseBuilder._INFO_PATH_COL)

        database.execute(cmd)
        values = [None, 'Version', version, path]
        database.insert_list(DatabaseBuilder._INFO_TABLE, values)

    @staticmethod
    def chunks(iterable, chunksize):
        """Yield successive n-sized chunks from iterable."""
        for i in range(0, len(iterable), chunksize):
            yield iterable[i:i + chunksize]

    @staticmethod
    def _encode(header, use_private_tags=False):
        # pydicom header to dictionary with (json) encoded values
        return sdtk.Header.from_pydicom_header(
                header, use_private_tags=use_private_tags)


