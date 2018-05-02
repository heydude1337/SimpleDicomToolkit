"""
Created on Tue Sep  5 16:54:20 2017

@author: HeyDude
"""


import pydicom
import json
from SimpleDicomToolkit.logger import Logger
import logging



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
            header = pydicom.read_file(file, stop_before_pixels=True)
        except pydicom.errors.InvalidDicomError:
            logger.error('Could not read file: {0}'.format(file))
            return None
        return Header.from_pydicom_header(header)

    @staticmethod
    def from_pydicom_header(header):
        hdict = Parser.encode(header)
        return Header.from_dict(hdict)


    def to_pydicom_header(self):
        return Parser.dataset_from_dict(self)

class Parser():
    """ Set of functions to convert pydicom headers to sqlite3 compatible dicts
    """
    _PRIVATE_TAG_PREFIX = 'private_tag_'
    _PRIVATE_TAG_NAME = _PRIVATE_TAG_PREFIX + '{group}_{element}_{VR}'


    @property
    def dictionary_tag(self):
        """ Complementary to the pydicom.datadict functions. Dictionary
        returns tag for keyword. """

        # lazy instanciation
        try:
            tag_dict = self._tag_dict
        except AttributeError:
            tag_dict = {}
            for tag, item in pydicom.datadict.DicomDictionary.items():
                tag_dict[item[-1]] = tag
                self._tag_dict = tag_dict

        return tag_dict


    @staticmethod
    def encode(dicom_header, skip_private_tags=False):
        """ Convert a pydicom header to a dictionary with encoded names
        as keys and (json) encoded values as values. """

        dicom_dict = {}

        # this triggers something in pydicom otherwise it messes up the headers
        try:
            repr(dicom_header)
        except:
            pass

        for element in dicom_header.values():
            tag  = element.tag

            if tag == (0x7fe0, 0x0010):
                # discard pixel data
                continue
            if skip_private_tags and tag.is_private:
                # skip private tags
                continue

            name, value = Parser.encocode_element(element, skip_private_tags)

            dicom_dict[name] = value

            # convert dictionary to header object to enable . indexing of dict
            dicom_dict = Header.from_dict(dicom_dict)

        return dicom_dict

    @staticmethod
    def encocode_element(element, skip_private_tags=False):
        """ Return encoded name and encoded value for a pydicom element """
        name = Parser._encode_tagname(element)
        value = Parser._encode_value(element, skip_private_tags)
        return name, value

    @staticmethod
    def encode_value_with_tagname(tagname, value):
        # TO DO, used to format values used in a query.
        return json.dumps(value)

    @staticmethod
    def _encode_tagname(element):
        # Get a valid name for sqlite3 column name. Private tags are encoded
        # in a specific format. Public tags are encoded as the pydicom keyword

        if element.keyword == '':
            # private or unknown tag
            name = Parser._PRIVATE_TAG_NAME
            name = name.format(group = str(hex(element.tag.group)),
                               element = str(hex(element.tag.element)),
                               VR = element.VR)
        else:
            name = element.keyword
        return name

    @staticmethod
    def _encode_value(element, skip_private_tags):
        # encode the value of a pydicom element to a json string
        if isinstance(element.value, pydicom.sequence.Sequence):
            # iterate over sequences each element of a sequence is a dataset
            values = []
            for seq in element:
                values += [Parser.encode(seq, skip_private_tags)]
            value = json.dumps(values)

        else:
            if isinstance(element.value, pydicom.multival.MultiValue):
                # convert to list before dump
                value = json.dumps([vi for vi in element.value])
            elif isinstance(element.value, pydicom.valuerep.PersonName3):
                # special treatment of person names
                value = json.dumps(element.value.original_string)
            elif isinstance(element.value, bytes):
                # bytes are converted to hex string
                value = json.dumps(element.value.hex())
            elif not isinstance(element.value, (int, float)):
                value = json.dumps(element.value)
            elif isinstance(element.value, pydicom.tag.BaseTag):
                # TO DO
                value = json.dumps(element.value.real)
            else:
                value = element.value
        return value

    @staticmethod
    def decode(header_dict):
        """ Convert dictionary to pydicom dataset. """
        ds = pydicom.Dataset()
        for tagname, repval in header_dict.items():
            value, tag, vr = Parser.decode_entry(tagname, repval)
            ds.add_new(tag, vr, value)

            if  ds[tag].VR == 'US or SS' and isinstance(value, int):
                ds[tag].VR = 'US' # force int to prevent invalid header

        # additional tags needed to write dataset to disk
        ds.is_little_endian = True
        ds.is_implicit_VR=False
        return ds

    @staticmethod
    def decode_entry(tagname, value):
        tag, vr = Parser._decode_tagname(tagname)
        value = Parser._decode_value(value, VR=vr)
        return value, tag, vr

    @staticmethod
    def _decode_tagname(tagname):
        if tagname in Parser().dictionary_tag.keys():
            tag = Parser().dictionary_tag[tagname]
            vr = pydicom.datadict.dictionary_VR(tag)
        else:
            tagname = tagname.replace(Parser._PRIVATE_TAG_PREFIX, '')
            group, elem, vr = tagname.split(sep='_')
            tag = pydicom.tag.Tag(group, elem)
        return tag, vr

    @staticmethod
    def _decode_value(dictvalue, VR = None):
        if isinstance(dictvalue, str):
            value = json.loads(dictvalue)
        elif isinstance(dictvalue, (float, int)):
            value = dictvalue
        elif dictvalue is None:
            value=dictvalue
        else:
            msg = 'Value should be str, int or float. Not {0}'
            raise TypeError(msg.format(str(type(dictvalue))))

        if isinstance(value, list) and VR == 'SQ':
            return [Parser.decode(vi) for vi in value]
        elif VR == 'OB':
            # bytes are stored as hex string, this should retrun bytes
            value = bytearray.fromhex(value).decode()
        elif VR == 'AT' and value is not None:
            # pydicom tags are stored as the real value
            value = pydicom.tag.Tag(value)
        elif VR == 'US or SS':
            value=str(value)
        return value
