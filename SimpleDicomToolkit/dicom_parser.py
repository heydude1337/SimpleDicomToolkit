"""
Created on Tue Sep  5 16:54:20 2017

@author: HeyDude
"""

import warnings
import logging
import json
from datetime import datetime

import dateutil
import pydicom
from SimpleDicomToolkit.logger import Logger


ISO_DATE = '%Y-%m-%d'
ISO_TIME = "%H:%M:%S.%f"
ISO_DATETIME = "%Y-%m-%d %H:%M:%S.%f"
DICOM_DATE = '%Y%m%d'
DICOM_DATETIME = '%Y%m%d %H%M%S.%f'
DICOM_TIME = '%H%M%S.%f'
LOGGER = Logger(app_name = 'dicom_parser', log_level=logging.ERROR).logger

space = 'SpacE'
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
        """ Header from normal dictionary """
        return Header(**hdict)

    @staticmethod
    def from_file(file):
        """ Return Header from dicom file """
        try:
            header = pydicom.read_file(file, stop_before_pixels=True)
        except pydicom.errors.InvalidDicomError:
            LOGGER.error('Could not read file: %s', file)
            return None
        return Header.from_pydicom_header(header)

    @staticmethod
    def from_pydicom_header(header, use_private_tags=False):
        """ Return Header from pydicom dataset """
        hdict = Encoder.encode(header, use_private_tags=use_private_tags)
        return Header.from_dict(hdict)


    def to_pydicom_header(self):
        """ Convert Header to a pydicom dataset """
        return Header.dataset_from_dict(self)

class Encoder():
    """ Convert pydicom header to dictionary with sqlite3 compatible values """
    _PRIVATE_TAG_PREFIX = 'private_tag_'
    _PRIVATE_TAG_NAME = _PRIVATE_TAG_PREFIX + '{group}_{element}_{VR}_{VM}'

    DT_NULL = datetime(1800, 1, 1).strftime(ISO_DATETIME)
    DA_NULL = datetime(1800, 1, 1).strftime(ISO_DATE)
    TM_NULL = '-1'

    @staticmethod
    def encode(dicom_header, use_private_tags=False):
        """ Convert a pydicom header to a dictionary with encoded names
        as keys and (json) encoded values as values. """

        dicom_dict = {}

        # this triggers something in pydicom otherwise it messes up the headers
        try:
            repr(dicom_header)
        except:
            pass

        for element in dicom_header.values():
            tag = element.tag
            
            if not use_private_tags and tag.is_private:
                # skip private tags
                continue
            
            if tag == (0x7fe0, 0x0010):
                # discard pixel data
                continue
        
            encoded = Encoder.encocode_element(element)

            if encoded is None:
                continue # skip element that failed encoding
            else:
                name, value = encoded

            dicom_dict[name] = value

            # convert dictionary to header object to enable . indexing of dict
            dicom_dict = Header.from_dict(dicom_dict)

        return dicom_dict

    @staticmethod
    def encocode_element(element):
        """ Return encoded name and encoded value for a pydicom element """
        name = Encoder._encode_tagname(element)
        if isinstance(element.value, pydicom.sequence.Sequence):
            value = Encoder._encode_sequence(element)
        else:
            try:
                value = Encoder._encode_value(element)
            except (TypeError, ValueError):
                msg = '\nCannot encode {0}, ommitting tag\n'
                warnings.warn(msg.format(element), RuntimeWarning)
                raise
                return None
            
        return name, value

    @staticmethod
    def encode_value_with_tagname(tagname, value):
        """ Convert a value corresponding with tagname """
        _, VR, VM = Decoder.decode_tagname(tagname)

        return Encoder.convert_value(value, VR)

    @staticmethod
    def _encode_tagname(element):
        # Get a valid name for sqlite3 column name. Private tags are encoded
        # in a specific format. Public tags are encoded as the pydicom keyword

        if element.keyword == '':
            # private or unknown tag
            name = Encoder._PRIVATE_TAG_NAME
            name = name.format(group=str(hex(element.tag.group)),
                               element=str(hex(element.tag.element)),
                               VR=element.VR,
                               VM=element.VM)
            # spaces are not handled properly in SQL statements
            name = name.replace(' ', space)
        else:
            name = element.keyword
        return name

    @staticmethod
    def _encode_sequence(seqs):

        # iterate over sequences each element of a sequence is a dataset
        values = []
        for seq in seqs:
            values += [Encoder.encode(seq)]
        value = json.dumps(values)
        return value

    @staticmethod
    def _encode_value(element):
        # encode the value of a pydicom element to a json string

        vr = element.VR
        vm = element.VM
        if Encoder.is_multiple(element.VM):
            # convert to list before converting elements in list

            value = json.dumps([Encoder.convert_value(vi, VR=vr, VM=vm)\
                                for vi in element.value])

        else:
            value = Encoder.convert_value(element.value, VR=vr, VM=vm)
        return value

    @staticmethod
    def convert_value(value, VR='', VM='1'):
        """ Convert a value to a sqlite3 compatible value. Most values will
        be converted to json strings. """
        if isinstance(value, pydicom.valuerep.PersonName3):
            # special treatment of person names
            
            if isinstance(value.original_string, bytes):
                value = json.dumps(value.original_string.decode())
            else:
                value = json.dumps(value.original_string)
        elif VR == 'DA': # DATE
            if value == '':
                value = Encoder.DA_NULL
            else:
                value = dateutil.parser.parse(value).strftime(ISO_DATE)
        elif VR == 'DT': # DATE AND TIME
            if value == '':
                value = Encoder.DT_NULL
            else:
                try:
                    value = dateutil.parser.parse(value).strftime(ISO_DATETIME)
                except:
                    pass # store original string, dicom dt fields are messy
        elif VR == 'TM': # TIME
            # remove : from non iso datetime formats
            value = value.replace(':', '')
            if value == '':
                return Encoder.TM_NULL
            elif '.' in value:
                value = datetime.strptime(value, '%H%M%S.%f')
            else:
                value = datetime.strptime(value, '%H%M%S')
            if value != Encoder.TM_NULL:
                value = value.strftime(ISO_TIME)

        elif VR == 'AT':
            # dicom tag reference
            value = json.dumps(value.real)
        elif isinstance(value, bytes):
            # bytes are converted to hex string
            value = json.dumps(value.hex())
        else:
            # Convert also ints and floats to string, stored as text in
            # database for simplicity. All columns are text.
            value = json.dumps(value)

        return value

    @staticmethod
    def is_multiple(VM):
        """ Return true if VM is greater than 1. VM can be integer or string.
        """
        # strings are in format '1-n'
        if isinstance(VM, int):
            return True if VM > 1 else False
        elif isinstance(VM, str):
            return True if ('-' in VM or float(VM) > 1) else False

class Decoder():
    """ Set of functions to convert sqlite3 compatible dicts to pydicom header
    """
    _tag_dict = None
    @property
    def dictionary_tag(self):
        """ Complementary to the pydicom.datadict functions. Dictionary
        returns tag for keyword. """

        # lazy instanciation
        if not self._tag_dict:
            tag_dict = {}
            for tag, item in pydicom.datadict.DicomDictionary.items():
                tag_dict[item[-1]] = tag
                self._tag_dict = tag_dict
        return tag_dict

    @staticmethod
    def decode(header_dict):
        """ Convert dictionary to pydicom dataset. """
        ds = pydicom.Dataset()
        for tagname, repval in header_dict.items():
            try:
                value, tag, vr, vm = Decoder.decode_entry(tagname, repval)
            except:
                raise ValueError('Cannot decode tag: {0} with value {1}'.format(tagname, repval))
            try:
                ds.add_new(tag, vr, value)
            except:
                warnings.warn('Cannot add tag {0} with value {1} and VR {2}'\
                              .format(tag, value, vr), RuntimeWarning)

            if  ds[tag].VR == 'US or SS' and isinstance(value, int):
                ds[tag].VR = 'US' # force int to prevent invalid header

        # additional tags needed to write dataset to disk
        ds.is_little_endian = True
        ds.is_implicit_VR = False
        return ds



    @staticmethod
    def decode_entry(tagname, value):
        """ Decode a value with a given (encoded) tagname """
        tag, vr, vm = Decoder.decode_tagname(tagname)

        if Decoder.is_sequence(tagname):
            # encoded sequence, recursive call
            value = [Decoder.decode(vi) for vi in json.loads(value)]
            return value, tag, vr, vm


        if Decoder.is_multiple(value):
            value = json.loads(value)
            value = [Decoder._decode_value(vi, VR=vr, VM=vm) for vi in value]
        else:
            value = Decoder._decode_value(value, VR=vr, VM=vm)

        # HACK, Hermes stores sometimes these weird values
        if isinstance(value, str) and value == '-1.$':
            value = 0
        return value, tag, vr, vm

    @staticmethod
    def decode_tagname(tagname):
        """ Return a pydicom tag, the VR and VM for a given tagname """
        if tagname in Decoder().dictionary_tag.keys():
            tag = Decoder().dictionary_tag[tagname]
            vr = pydicom.datadict.dictionary_VR(tag)
            vm = pydicom.datadict.dictionary_VM(tag)
        else:
            tagname = tagname.replace(Encoder._PRIVATE_TAG_PREFIX, '')
            group, elem, vr, vm = tagname.split(sep='_')
            tag = pydicom.tag.Tag(group, elem)
            # spaces are not handled properly in SQL statements
            vr = vr.replace(space, ' ')
        return tag, vr, vm

    @staticmethod
    def _decode_value(value, VR=None, VM='1'):
        if isinstance(value, str) and VR not in ('DA', 'DT', 'TM'):
            value = json.loads(value)

        if value is None:
            pass
        elif isinstance(value, list) and VR == 'SQ':
            return [Decoder.decode(vi) for vi in value]
        elif VR == 'OB':
            # bytes are stored as hex string, this should retrun bytes
            value = bytearray.fromhex(value)
        elif VR == 'AT' and value is not None:
            # pydicom tags are stored as the real value
            value = pydicom.tag.Tag(value)
        elif VR == 'US or SS':
            value = str(value)
        elif VR == 'DA':
            if value == Encoder.DA_NULL:
                value = ''
            else:
                value = dateutil.parser.parse(value).strftime(DICOM_DATE)
        elif VR == 'DT':
            if value == Encoder.DT_NULL:
                value = '.'
            else:
                try:
                    value = dateutil.parser.parse(value).strftime(DICOM_DATETIME)
                except:
                    pass # return original string, dicom DT fields are messy
        elif VR == 'TM':
            if value == Encoder.TM_NULL:
                value = ''
            else:
                value = datetime.strptime(value, ISO_TIME).strftime(DICOM_TIME)
        return value

    @staticmethod
    def is_sequence(tagname):
        """ Return True if tagname is accompanied by a DICOM sequence """
        _, VR, _ = Decoder.decode_tagname(tagname)
        return VR == 'SQ'

    @staticmethod
    def is_multiple(converted_value):
        """ Return True if value contains multiple values """
        try:
            value = json.loads(converted_value)
            if isinstance(value, list):
                return True
            else:
                return False
        except:
            return False

def test_encode(file):
    try:
        header = pydicom.read_file(file)
    except:
        print('Pydicom Failed to read file(!)')
        
    for element in header.values():
        tag = element.tag

        if tag == (0x7fe0, 0x0010):
            # discard pixel data
            continue
        try:
            Encoder.encocode_element(element)
        except:
            print('Failed encoding {0} with value {1}'.format(str(tag), str(header[tag])))
            break
    return tag, header[tag]
            



