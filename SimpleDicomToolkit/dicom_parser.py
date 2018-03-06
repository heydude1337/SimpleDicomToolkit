"""
Created on Tue Sep  5 16:54:20 2017

@author: HeyDude
"""

try:
    import pydicom as dicom
except ImportError:
    import dicom

import dateutil
import json
from collections import OrderedDict

from SimpleDicomToolkit import dicom_date_time
from SimpleDicomToolkit.read_dicom import DicomReadable
from SimpleDicomToolkit.logger import Logger, logging


VR_STRING   = ('AE', 'AS', 'AT', 'CS', 'LO', 'LT', 'OB', 'OW', \
                   'SH', 'ST', 'UI', 'UN', 'UT') # stored as string
VR_PN       = 'PN'
VR_DATE     = 'DA'
VR_DATETIME = 'DT'
VR_TIME     = 'TM'
VR_FLOAT    = ('DS', 'FL', 'FD', 'OD') #DS is unparsed, FL and FD are floats
VR_INT      = ('IS', 'SL', 'SS', 'UL', 'US')
VR_SEQ      = 'SQ'

logger = Logger(app_name = 'dicom_parser', log_level = logging.ERROR).logger

class Header(dict):
    """ Wrapper for dictionary enabling the usage of keys as attributes """
    def __init__(self, *args, **kwargs):
        super(Header, self).__init__()
        self.update(*args, **kwargs) # mandatory for a subclass of dict

    def __dir__(self):
        # enable autocomplete
        res = dir(type(self)) + list(self.__dict__.keys())
        res += list(self.keys())
        return res

    def __getattr__(self, attr):
        # find key in dictionary and return value as attribute
        if attr in self.keys():
            return self[attr]
        else:
            raise AttributeError

    @staticmethod
    def from_dict(hdict):
        return Header(**hdict)

    @staticmethod
    def from_file(file):
        try:
            header = dicom.read_file(file, stop_before_pixels=True)
        except dicom.errors.InvalidDicomError:
            logger.error('Could not read file: {0}'.format(file))
            return None
        return Header.from_pydicom_header(header)

    @staticmethod
    def from_pydicom_header(header):
        hdict = Parser.dicom_dataset_to_dict(header)
        return Header.from_dict(hdict)


class DicomFiles(OrderedDict, DicomReadable):
    """ Wrapper around a dictionary of dicom files, where the keys
    are the file names and the values are the (parsed) headers. """
    def __init__(self, *args, **kwargs):
        super(DicomFiles, self).__init__()
        # dictionary = kwargs.pop('dictionary', None) # convert dict
        self.update(*args, **kwargs) # mandatory for a subclass of dict

        # list tags present in the tags of the dicom headers
        tag_names = set()
        for value in self.values():
            for key in value.keys():
                tag_names.add(key)

        self.tag_names = tag_names

    @property
    def files(self):
        """ List containing all file names included in the database """
        return list(self.keys())

    @property
    def headers(self):
        """ List containing the header for each file in the database """
        return list(self.values())

    @property
    def isempty(self):
        """ True if the database is empty """
        return len(self) == 0

    def __iter__(self):
        self.n = 0
        return self

    def __next__(self):
        if self.n >= len(self):
            raise StopIteration
        item = self.query(SeriesInstanceUID = self.SeriesInstanceUID[self.n])
        self.n += 1
        return item

    def sort(self, sort_by_tag):
        """ Sort files based on values of a specific tag. """
        sort_values = self.get_values_for_tag(sort_by_tag, unique=False)
        index = _argsort(sort_values)
        return self._select_by_index(index)

    def _select_by_index(self, index):
        if not isinstance(index, (tuple, list)):
            index = [index]
        files = list(self.keys())
        values = list(self.values())

        selected = OrderedDict([(files[i], values[i]) for i in index])
        # DicomFiles must also subclass OrderedDict for this to work
        return DicomFiles(**selected)

    def filter(self, tag, value=None):
        """Filter dictionary based on values for a specific tag """
        tag_values = self.get_values_for_tag(tag, unique=True)

        if value is None:
            return [self.filter(tag, value=v) for v in tag_values]
        else:
            index = [i for i, v in enumerate(tag_values) if v == value]
            return self._select_by_index(index)


    def query(self, partial_match=False, **kwargs):
        """ Perform a query based on attribute values for dicom Header objects.
        """
        selected = self
        for tag, value in kwargs.items():
            selected = selected._select_by_value(tag_name=tag, tag_value=value,
                                             partial_match=partial_match)

        selected.SUV = self.SUV # HACK to communucate value type to reader
        return selected

    def _select_by_value(self, tag_name=None, tag_value=None,
                         partial_match=False):

        if partial_match:
            match = lambda a, b: a in b
        else:
            match = lambda a, b: a == b

        if tag_name not in self.tag_names:
            print('Cannot use {0} in selection!'.format(tag_name))
            raise KeyError

        files = []
        for file, value in self.items():
            if tag_name in value.keys():
                if match(tag_value, value[tag_name]):
                    files += [file]

        selected = dict([(k, v) for k, v in self.items() if k in files])
        selected = DicomFiles(**selected)
        return selected

    def get_values_for_tag(self, tag, unique=True):
        """ Get all values for a given tag name, removes duuplicates if unique is
        True."""
        if(tag) not in self.tag_names:
            raise AttributeError

        tag_values = []
        for value in self.values():
            if tag in value.keys():
                tag_values += [value[tag]]
            else:
                tag_values += [None]
        if unique:
            tag_values = DicomFiles._unique_list(tag_values)

        tag_values = Parser.parse_values(tag_values, tag)

        if len(tag_values) == 1:
            tag_values = tag_values[0]

        return tag_values

    def __dir__(self):
        # enable autocomplete
        res = dir(type(self)) + list(self.__dict__.keys())
        res += list(self.tag_names)
        return res

    def __getattr__(self, attr):
        if(attr) not in self.tag_names:
            # print(attr + ' not valid')
            raise AttributeError
        else:
            attr_value = self.get_values_for_tag(attr)

        return attr_value

    @staticmethod
    def _unique_list(l):
        # Get Unique items in list while preserving ordering of elements
        seen = set()
        seen_add = seen.add

        unique_list = [x for x in l if not (str(x) in seen or seen_add(str(x)))]

        return unique_list

    @staticmethod
    def from_folder(folder):
        files = DicomFiles.files_in_folder(folder, recursive = True)
        return DicomFiles.from_files(files)

    @staticmethod
    def from_files(files):

        dicom_files = []
        headers = []
        for file in files:
            header = Header.from_file(file)
            # if file is not dicom header will be None, skip
            if header is not None:
                headers += [header]
                dicom_files += [file]

        return DicomFiles(zip(dicom_files, headers))

def _argsort(seq):
    # http://stackoverflow.com/questions/3071415/efficient-method-to-calculate-the-rank-vector-of-a-list-in-python
    return sorted(range(len(seq)), key=seq.__getitem__)

class Parser():
    """ Set of functions to convert pydicom headers to sqlite3 compatible dicts
    """
    @staticmethod
    def dicom_dataset_to_dict(dicom_header, skip_private_tags=False):
        """ Convert a pydicom header to a dictionary. """
        # TO DO MAKE TAG NAMES MORE COMPLIANT WITH SQLITE3 COLUMN NAMES

        dicom_dict = {}

        # this triggers something in pydicom otherwise it messes up the headers
        try:
            repr(dicom_header)
        except:
            pass

        for dicom_value in dicom_header.values():
            tag  = dicom_value.tag

            if tag == (0x7fe0, 0x0010):
                # discard pixel data
                continue
            if skip_private_tags and tag.is_private:
                # skip private tags
                continue

            name = Parser.sqlite3_name_for_tag(tag)
            value = Parser.sqlite3_value(dicom_value)

            dicom_dict[name] = value

            # convert dictionary to header object to enable . indexing of dict
            dicom_dict = Header.from_dict(dicom_dict)

        return dicom_dict

    @staticmethod
    def parse_values(values, tag):
        """ Parse json string from database, decode and convert date time fields to
        a datetime object. If values is tuple, list or dict, all values will be
        converted. """

        # recursive call for list and dicts
        if isinstance(values, dict):
            return Header([(k, Parser.parse_values(v, k)) for k, v in values.items()])
        elif isinstance(values, (tuple, list)):
            return [Parser.parse_values(v, tag) for v in values]


        # decode
        elif isinstance(values, str):
            # decode json string
            try:
                values = json.loads(values)
            except (ValueError, TypeError):
                pass

            # if json string is decoded as dict, list or tuple do a recursive call
            # this ensures decoding of date time values in child objects
            if isinstance(values, (dict, list, tuple)):
                return Parser.parse_values(values, tag)

            elif isinstance(values, str):
                # decode date time depending on the tag name
                if tag is not None and (('Time' in tag) or ('Date' in tag)):
                    try:
                        values = dateutil.parser.parse(values)
                    except (ValueError, TypeError):
                        pass
                # retrurn decoded value
                return values
            else:
                # return decoded non string, list, tuple and dict values
                return values
        else:
            # no parsing possible just return the values
            return values

    @staticmethod
    def tag_name_for_group_and_element(group, element):
        # Name tags by element and group number
        return 'T_{0}_{1}'.format(hex(group), hex(element))

    @staticmethod
    def sqlite3_name_for_tag(tag):
        # Return a string name for tag that can be used as column name is sqlite3

        valid_tag_name = lambda name: isinstance(name, str) and name.strip() != ''

        if not tag.is_private:
            try:
                name = dicom.datadict.all_names_for_tag(tag)[0]
            except:
                name = ''
        elif tag.is_private:
            # use tag notation as name (0x0000, 0x0000)
            name = Parser.tag_name_for_group_and_element(tag.group, tag.element)

        # HACK
        if not valid_tag_name(name):
            msg = 'no valid name foud for {0}'.format(repr(tag))
            logger.info(msg)
            name = Parser.tag_name_for_group_and_element(tag.group, tag.element)
        return name

    @staticmethod
    def sqlite3_value(dicom_value):
        # convert dicom value to number or json string so it can be stored in sqlite3
        if isinstance(dicom_value, dicom.dataelem.DataElement):
            if dicom_value.VR == 'SQ':

                # loop through each item in a sequence
                cv = []
                for v in dicom_value:
                    cv += [Parser.dicom_dataset_to_dict(v)] # recursive call

            else:
                try:
                    cv = Parser._convert_value(dicom_value.value, dicom_value.VR)
                except:
                    msg = 'Could not convert {0} for tag {1}'
                    logger.info(msg.format(dicom_value.value,
                                                dicom_value.tag))

                    try:
                        cv = str(cv)
                    except:
                        cv = None

        elif isinstance(dicom_value, dicom.dataset.Dataset):
            # recursive call for each item of a sequence
            cv = Parser.dicom_dataset_to_dict(dicom_value, dicom_value.VR)

        else:
            msg = 'Could not convert {0} for tag {1}, setting value to None'
            logger.error(msg.format(dicom_value.value, dicom_value.tag))
            cv = None
        return cv

    @staticmethod
    def _sanitise_unicode(s):
        return s.replace(u"\u0000", "").strip()

    @staticmethod
    def _convert_value(v, VR):
        t = type(v)
        VR = VR[0:3]
        if t == dicom.valuerep.MultiValue or t == list:
            # recursive call for list values
            cv = [Parser._convert_value(vi, VR) for vi in v]
        elif VR == VR_DATE:
            cv = dicom_date_time.format_date(v)
        elif VR == VR_DATETIME:
            cv = dicom_date_time.format_datetime(v)
        elif VR == VR_TIME:
            cv = dicom_date_time.format_time(v)
        elif VR in VR_STRING and t != bytes:
            cv = Parser._sanitise_unicode(v)
        elif VR == VR_PN:
            cv = str(v)
        elif VR in VR_FLOAT:
            cv = float(v)
        elif VR in VR_INT:
            cv = int(v)
        elif t == bytes:
            s = v.decode('ascii', 'replace')

            cv = Parser._sanitise_unicode(s)
        else:
            raise ValueError('VR Type is not Known: ' + VR)

        if VR in (VR_DATE, VR_TIME, VR_DATETIME):
            if cv != '' and not isinstance(cv,list):
                cv = cv.isoformat()
        return cv
