"""
Created on Tue Sep  5 16:54:20 2017

@author: HeyDude
"""



from collections import OrderedDict
import dicom

from SimpleDicomToolkit import dicom_date_time
from SimpleDicomToolkit.read_dicom import DicomReadable


VR_STRING   = ('AE', 'AS', 'AT', 'CS', 'LO', 'LT', 'OB', 'OW', \
                   'SH', 'ST', 'UI', 'UN', 'UT') # stored as string
VR_PN       = 'PN'
VR_DATE     = 'DA'
VR_DATETIME = 'DT'
VR_TIME     = 'TM'
VR_FLOAT    = ('DS', 'FL', 'FD', 'OD') #DS is unparsed, FL and FD are floats
VR_INT      = ('IS', 'SL', 'SS', 'UL', 'US')
VR_SEQ      = 'SQ'

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
            return None
        return Header.from_pydicom_header(header)

    @staticmethod
    def from_pydicom_header(header):
        hdict = dicom_dataset_to_dict(header)
        return Header.from_dict(hdict)

#    @staticmethod
#    def factory(iterable):
#        """ Loop through an iterable that contains dicom dictionaries, convert each
#        header to a Header object """
#
#        if isinstance(iterable, dict) and not isinstance(iterable, Header):
#            # run recursive on all items in dict
#            iterable = dict([(k, Header.factory(v)) for k, v in iterable.items()])
#            return Header.from_dict(iterable)
#        elif isinstance(iterable, (list, tuple)):
#            return [Header.factory(item) for item in iterable]
#        else:
#            return iterable



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

    def sort(self, sort_by_tag):
        """ Sort files based on values of a specific tag. """
        sort_values = self.get_values_for_tag(sort_by_tag, unique=False)
        index = _argsort(sort_values)
        return self._select_by_index(index)

    def _select_by_index(self, index):
        if isinstance(index, (tuple, list)):
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
            selected = self._select_by_value(tag_name=tag, tag_value=value,
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

        if len(attr_value) == 1:
            attr_value = attr_value[0]

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

def dicom_dataset_to_dict(dicom_header, skip_private_tags=True):
    """ Convert a pydicom header to a dictionary. """

    dicom_dict = {}

    # this triggers something in pydicom otherwise it messes up the headers
    repr(dicom_header)

    for dicom_value in dicom_header.values():
        value = dicom_value.value
        tag  = dicom_value.tag
        try:
            name = dicom.datadict.all_names_for_tag(tag)[0]
        except:
            name = 'none'

        if dicom_value.tag == (0x7fe0, 0x0010):
            # discard pixel data
            continue
        if skip_private_tags and tag.is_private:
            # skip private tags
            continue
        elif tag.is_private:
            # use tag notation as name (0x0000, 0x0000)
            name = repr(tag)
        else:
            # use pydicom to get name for tag
            name = dicom.datadict.all_names_for_tag(tag)[0]

        if isinstance(dicom_value, dicom.dataelem.DataElement):

            if dicom_value.VR == 'SQ':

                # loop through each item in a sequence
                cv = []
                for v in value:
                    cv += [dicom_dataset_to_dict(v)] # recursive call

                dicom_dict[name] = cv
            else:
                try:
                    cv = _convert_value(dicom_value.value, dicom_value.VR)
                except ValueError:
                    msg = 'Could not convert {0} for tag {1}'
                    print(msg.format(dicom_value.value, dicom_value.tag))
                    cv = None
                dicom_dict[name] = cv

        elif isinstance(dicom_value, dicom.dataset.Dataset):
            # recursive call for each item of a sequence
            v = dicom_dataset_to_dict(dicom_value, dicom_value.VR)
            dicom_dict[dicom_value.tag] = v

        # convert dictionary to header object to enable . indexing of dict
        dicom_dict = Header.from_dict(dicom_dict)

    return dicom_dict


def _sanitise_unicode(s):
    return s.replace(u"\u0000", "").strip()


def _convert_value(v, VR):
    t = type(v)
    VR = VR[0:3]
    if t == dicom.valuerep.MultiValue or t == list:
        # recursive call for list values
        cv = [_convert_value(vi, VR) for vi in v]
    elif VR == VR_DATE:
        cv = dicom_date_time.format_date(v)
    elif VR == VR_DATETIME:
        cv = dicom_date_time.format_datetime(v)
    elif VR == VR_TIME:
        cv = dicom_date_time.format_time(v)
    elif VR in VR_STRING and t != bytes:
        cv = _sanitise_unicode(v)
    elif VR == VR_PN:
        cv = str(v)
    elif VR in VR_FLOAT:
        cv = float(v)
    elif VR in VR_INT:
        cv = int(v)
    elif t == bytes:
        s = v.decode('ascii', 'replace')
        cv = _sanitise_unicode(s)
    else:
        print(VR)
        raise ValueError

    if VR in (VR_DATE, VR_TIME, VR_DATETIME):
        if cv != '' and not isinstance(cv,list):
            cv = cv.isoformat()
    return cv
