# -*- coding: utf-8 -*-
"""
Created on Tue Sep  5 16:54:20 2017

@author: HeyDude
"""
import os
import json
import pydicom
import logging
import warnings

import SimpleDicomToolkit as sdtk
import SimpleITK as sitk

# warnings.simplefiter('always')
VERSION = 0.91

class Database(sdtk.Logger):

    """ Creates a Sqlite3 table from a list of dicom files. Each header entry
    is stored in a seperate column. Sequences are stored in a single column """

    _path           = None
    _LOG_LEVEL      = logging.INFO
    _DATABASE_FILE  = 'minidicom.db'    # default file name for database
    _images         = None # cache images
    _image          = None # cache single image
    _headers        = None # cache headers
    _tag_names      = None # cache for tagnames in current selection
    MAX_FILES       = 5000 # max number of files to be read at by property images
    
    
    def __init__(self, path, rebuild=False, scan=True, silent=False, 
        SUV = True, in_memory=False):
        """ Create a dicom database from path

            rebuild: Deletes the database file and generates a new database
            scan:    Scans for all dicom files in the path and updates
                     the database. Missing files will be removed as well
            silent:  Supress progressbar and log messages except errors

        """
        
        self.builder = DatabaseBuilder(path=path, scan=scan, rebuild=rebuild, 
                                       in_memory=in_memory)
        
        self.database = self.builder.database
        
        self.SUV = SUV
        
        if silent:
            self._LOG_LEVEL = logging.ERROR
            
        self.reset() #Ensure all files are selected, clear old selection.
        
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
        msg = ('Database with:\n'
               '\t- {npatients} patient(s)\n'
               '\t- {nstudies} studie(s)\n'
               '\t- {nseries} serie(s)\n'
               '\t- {ninstances} instance(s)')
        
        msg = msg.format(npatients = self.patient_count,
                         nstudies = self.study_count,
                         nseries = self.series_count,
                         ninstances = self.instance_count)
        
        return msg

    def __repr__(self):
        return self.__str__()
        
    
    @property
    def files(self):
        """ Retrieve all files with  path from the database """
        return self.get_column(self.builder._FILENAME_COL, close=True)
    
    @property
    def columns(self):
        return self.database.column_names(self.builder._MAIN_TABLE, close=True)
    
    @property
    def non_tag_columns(self):
        return (self.builder._FILENAME_COL, 
                self.builder.database.ID, 
                self.builder._SELECT_COL,
                self.builder._TAGNAMES_COL)
   
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
        
            
        self._headers = headers
                
        return self._headers
    
    @property
    def header(self):
        if self.instance_count > 1:
            raise IndexError('Multiple series found!')
        return self._headers[0]
    
    @property
    def series_count(self):
        return self._count_tag('SeriesInstanceUID')
    
    @property
    def study_count(self):
        return self._count_tag('StudyInstanceUID')
    
    @property
    def patient_count(self):
        return self._count_tag('PatientID')
    
    @property
    def instance_count(self):
        return self._count_tag('SOPInstanceUID')
    
    @property
    def image(self):
        """ Returns an sitk image for the files in the files property.
            All files must belong to the same dicom series
            (same SeriesInstanceUID). """
        
        if self._image is not None:
            return self._image
        
        assert self.series_count == 1    
        
        if hasattr(self, 'SliceLocation'):
            sort_by = 'SliceLocation'
        elif hasattr(self, 'InstanceNumber'):
            sort_by = 'InstanceNumber'
        else:
            if self.instance_count > 1:
                warnings.warn('\nSlice Sorting Failed Before Reading!\n', 
                              RuntimeWarning)
            sort_by = None
            
        files = self.database.get_column_where(self.builder._MAIN_TABLE,
                                               self.builder._FILENAME_COL, 
                                               sort_by=sort_by,
                                               sort_decimal = True,
                                               active = True)
        
        image = sdtk.dicom_reader.read_serie(files, SUV=False, 
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
                self.SUV = False                
            pass
            
        if self.SUV:
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

        if len(self.files) > self.MAX_FILES:
            raise IOError('Number of files exceeds MAX_FILES property')

        if self._images is not None:
            return self._images
        
        assert hasattr(self, sdtk.SERIESINSTANCEUID)
        
        images = {}
        
        for uid in self.SeriesInstanceUID:
            images[uid] = self.query(SeriesInstanceUID=uid).image
        
        self._images = images
        return self._images
    
    @property
    def array(self):
        return sitk.GetArrayFromImage(self.image)
    
    @property
    def arrays(self):
        return dict([(key, sitk.GetArrayFromImage(image)) \
                     for key, image in self.images.items()])
    @property
    def _selected_rows(self):
        return self.database.get_row_dict(self.builder._MAIN_TABLE, 
                                          active=True)
    
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
        old_selection = self.files # store current selection
      
        self.deselect_all() # exclude everything from selection
        
        # encode key word arguments
        for tag, value in kwargs.items():
            
            if tag in self.non_tag_columns:
                continue
            

            value = self._encode_value(tag, value)       
            
            kwargs[tag] = value
        
        # make selection on all files
        self.database.set_column_where(self.builder._MAIN_TABLE, 
                                       self.builder._SELECT_COL, True, **kwargs)
        
        new_selection = self.files                           
        # gather files that are in the new selection but not in the old 
        # selection
        deselect = [file for file in new_selection\
                    if file not in old_selection]
        
        if deselect:           
            # deselect these files, selection should be a subset of the old
            # selction. Divide in chunks to prevent exceeding the number of 
            # '?' in an SQL statement.
            chunks = self.builder._chunks(deselect, 1000)
            for chunk in chunks:
                self.database.set_column_where(self.builder._MAIN_TABLE, 
                                               self.builder._SELECT_COL, False,
                                               **{self.builder._FILENAME_COL: deselect})        
        
        if close:
            self.database.close()
        
        self._reset_cache()
        
        return self
    
    def header_for_uid(self, sopinstanceuid):
        sopinstanceuid = sdtk.Encoder.encode_value_with_tagname('SOPInstanceUID',
                                                           sopinstanceuid)
        
        h_dicts = self.database.get_row_dict(self.builder._MAIN_TABLE, 
                                             SOPInstanceUID=sopinstanceuid)
        if len(h_dicts) == 0:
            msg = 'SOPInstanceUID {0} not in database'
            self.logger.info(msg.format(sopinstanceuid))
        elif len(h_dicts) > 1:
            msg = 'SOPInstanceUID {0} not unique'
            raise ValueError(msg.format(sopinstanceuid))
        h_dict = h_dicts[0]
        h_dict = {tag: h_dict[tag] for tag in self.tag_names}
        
        return self._decode(h_dict)
    
    def reset(self):
        """ After a query a subset of the database is visible, use reset
        to make all data visible again. """
       
        self._reset_cache()
        self.select_all()       # Make all active again
        
        return self

    def select_all(self):
        self.database.set_column(self.builder._MAIN_TABLE, 
                                 self.builder._SELECT_COL, True)
        self._reset_cache()
        
    def deselect_all(self):
        self.database.set_column(self.builder._MAIN_TABLE, 
                                 self.builder._SELECT_COL, False)
        self._reset_cache()
    
    def get_column(self, column_name, distinct=True,
                   sort=True, close=True, parse=True):
        """ Return the unique values for a column with column_name """
        if sort:
            sort_by = column_name
        else:
            sort_by = None
            
        values = self.database.get_column(self.builder._MAIN_TABLE, 
                                          column_name, sort_by=sort_by, 
                                          distinct=distinct, 
                                          active=True, close=False)
        
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
        tagname_rows = self.get_column(self.builder._TAGNAMES_COL, parse=False)
        tagnames = set()
        for row in tagname_rows:
            for tagname in json.loads(row):
                tagnames.add(tagname)
       
        return tuple(tagnames)
    
    def _reset_cache(self):
        # Clear stored values of this object 
        self._headers = None    # Clear cache
        self._images = None
        self._image = None
        self._tagnames = None   # Clear cache
        
    @staticmethod
    def _encode_value(tagname, value):
        _, VR, VM = sdtk.Decoder._decode_tagname(tagname)
        if sdtk.SQLiteWrapper.is_between_dict(value):
            for key, v in value.items():
                value[key] = sdtk.Encoder._convert_value(v, VR=VR, VM=VM)
        else:
            value = sdtk.Encoder._convert_value(value, VR=VR, VM=VM)
        return value
    
    @staticmethod
    def _decode(hdict):
        return sdtk.Decoder.decode(hdict)   
    
    def _count_tag(self, tagname):
        try:
            values = getattr(self, tagname)
        except AttributeError:
            values = [] 
        if not isinstance(values, (list, tuple)):
            count = 1
        else:
            count = len(values)
        return count
   
    
class DatabaseBuilder(sdtk.Logger):
    _FILENAME_COL    = 'dicom_file_name' # colum in table that stores filenames
    _TAGNAMES_COL    = 'dicom_tag_names' # column that stores tag names for file
    _SELECT_COL      = 'active'       # Select rows (True/False)
    
    _MAIN_TABLE      = 'DicomMetaDataTable'   # stores values for each tag
    _INFO_TABLE      = 'Info'                 # store database version
    _INFO_DESCRIPTION_COL = 'Description'
    _INFO_VALUE_COL = 'Value'
    _FILENAME_TABLE  = 'FileNameTable' # stores non dicom files
    
    _chunk_size     = 1000  # number of files to read before committing
    
    def __init__(self, path=None, scan=True, silent=False, database_file=None,
                  rebuild=False, in_memory=False):
                 
        self.path = self.parse_path(path)
        
        if database_file is None:
            database_file = self.get_database_file(database_file, 
                                                   in_memory=in_memory)
        self.database_file = database_file
            
        db = self.open_database(database_file=database_file, rebuild=rebuild)
                              
        
        self.database = db
        
        files = self.file_list(self.path, index=scan)
        
        self._update_db(files=files, silent=silent)
    
    @property
    def files(self):
        """ Return all files dicom and non dicom that were added or tried to 
        add to the database. These files will not be re-added."""
        return self.database.get_column(self._FILENAME_TABLE,
                                        self._FILENAME_COL)
        
    def open_database(self, database_file, rebuild=False):
        database = sdtk.SQLiteWrapper(database_file)
        database._LOG_LEVEL = self._LOG_LEVEL

        if rebuild:
            self.logger.info('Removing tables from: ' + database.database_file)
            database.delete_all_tables()
        elif self.get_version(database) < VERSION: 
            warnings.warn('Old database version, rebuilding may be necessary')
            if self._INFO_TABLE not in database.table_names:
                self._create_info_table(database, version=0)
            
        if not self._MAIN_TABLE in database.table_names:
            self._create_main_table(database)
        if not self._INFO_TABLE in database.table_names:
            self._create_info_table(database)
        if not self._FILENAME_TABLE in database.table_names:
            self._create_filename_table(database)
        return database
            
   
    def get_version(self, database):
        if self._INFO_TABLE not in database.table_names:
            v = 0
        else:
            v = database.get_column(self._INFO_TABLE, 
                                         self._INFO_VALUE_COL)[0]
      
        return float(v)
    
    def get_database_file(self, path, in_memory=False):
        # database file name
        if in_memory:
            database_file = sdtk.SQLiteWrapper.IN_MEMORY
        
        elif isinstance(path, str) and os.path.isdir(path):
            database_file = os.path.join(path, Database._DATABASE_FILE)
        else:
            path = os.getcwd()
            database_file = os.path.join(path, Database._DATABASE_FILE)
        return database_file
    
    def parse_path(self, path):
        if isinstance(path, str) and os.path.isdir(path):            
            # folder passed
            path = os.path.abspath(path)
        return path
    
    def file_list(self, path, index=True):
        # gather file list
        if index:
            files = sdtk.FileScanner.files_in_folder(path, recursive=True)
        else:
            files = []
        return files
                
    def insert_file(self, file, _existing_column_names=None, close=True):
        """ Insert a dicom file to the database """
        
        self.database.insert_row_dict(self._FILENAME_TABLE,
                                      {self._FILENAME_COL: file})
        
        if _existing_column_names is None:
            _existing_column_names = self.database.column_names(self._MAIN_TABLE,
                                                       close=False)

        # read file from disk
        fullfile = os.path.join(self.path, file)
     
        header = pydicom.read_file(fullfile, stop_before_pixels=True)


        # convert header to dictionary
        try:
            hdict = DatabaseBuilder._encode(header)
        except Exception as e:
            self.database.close()
            raise RuntimeError('Cannot add: {file}'.format(file=file))
            
        
        # store tag names        
        hdict[self._TAGNAMES_COL] = json.dumps(list(hdict.keys())) 
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
            self.database.close()
            raise IOError(msg)

        if close:
            self.database.close()

        self.logger.debug(newcols)
        #self._reset_cache()
        return newcols

    def remove_files(self, file_names):
        """ Remove file list from the database """
        for file_name in file_names:
            self.remove_file(file_name, close=False)

        self.database.close()
        
    def remove_file(self, file_name, close=True):
        """ Remove file from database """
       
        self.database.delete_rows(self.builder_MAIN_TABLE, column=self._FILENAME_COL,
                         value=file_name, close=False)
        
        self.database.delete_rows(self.builder._FILENAME_TABLE, column=self._FILENAME_COL,
                         value=file_name, close=False)

        if close:
            self.database.close()
        self._reset_cache()
        
    def _add_column_for_tags(self, tag_names, skip_check = False):
        # add columns to the databse for the given tag_names
        # the sqlite datatype will be determined from the dicom value
        # representation in the datadict of pydicom
        if not skip_check:
            existing_columns = self.database.column_names(self._MAIN_TABLE, 
                                                          close=False)
        else:
            existing_columns = []

        var_type = sdtk.SQLiteWrapper.TEXT # everything is stored as text
        
        for tag_name in tag_names:    
            if tag_name not in existing_columns:
                self.database.add_column(self._MAIN_TABLE, tag_name, 
                                close=False, var_type=var_type)
    
    def _update_db(self, files= [], existing_files=None, silent=False):
        # scan for file new and removed files in the path. Update the
        # database with new files, remove files that are no longer in path
        if files is None or len(files) == 0:
            return
        
        if existing_files is None:
            existing_files = self.files
            
        new_files, not_found = sdtk.FileScanner.compare(files, existing_files)
            
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
        batches = self._chunks(new_files, self._chunk_size)
        columns = self.database.column_names(self._MAIN_TABLE)
     
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
                         file_name=DatabaseBuilder._FILENAME_COL)
                       
        database.execute(cmd)
        
    @staticmethod
    def _create_main_table(database):
        # create the main table with dicom tags as columns
        cmd = """CREATE TABLE  IF NOT EXISTS {table}
                 (id INTEGER AUTO_INCREMENT PRIMARY KEY ,
                  {file_name} TEXT UNIQUE,
                  {tag_names} TEXT,
                  {selected} INTEGER)"""

        cmd = cmd.format(table=DatabaseBuilder._MAIN_TABLE,
                         file_name=DatabaseBuilder._FILENAME_COL,
                         tag_names=DatabaseBuilder._TAGNAMES_COL,
                         selected = DatabaseBuilder._SELECT_COL)

        database.execute(cmd)
    
    @staticmethod
    def _create_info_table(database, version=VERSION):
        database.logger.info('Create INFO Table with version: ' + str(version))
        cmd = """CREATE TABLE  IF NOT EXISTS {table}
                 (id INTEGER AUTO_INCREMENT PRIMARY KEY,
                  {info_descr} TEXT,
                  {info_val} TEXT) """
    
        cmd = cmd.format(table = DatabaseBuilder._INFO_TABLE, 
                         info_descr = DatabaseBuilder._INFO_DESCRIPTION_COL,
                         info_val = DatabaseBuilder._INFO_VALUE_COL)
        
        database.execute(cmd)
        values = [None, 'Version', version]
        database.insert_list(DatabaseBuilder._INFO_TABLE, values)
        
    @staticmethod
    def _chunks(iterable, chunksize):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(iterable), chunksize):
            yield iterable[i:i + chunksize]
    @staticmethod
    def _encode(header):
        # pydicom header to dictionary with (json) encoded values
        return sdtk.Header.from_pydicom_header(header)        
    
    
if __name__ == "__main__":
    folder = 'C:/Users/757021/Data/Orthanc'
    #folder = 'C:/Users/757021/Data/Y90'
    database = Database(path = folder, rebuild=False, scan=True)
    database.select(SeriesDescription='WB')
    self = database
    self.image
    