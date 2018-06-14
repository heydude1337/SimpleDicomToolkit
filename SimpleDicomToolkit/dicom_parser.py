"""
Created on Tue Sep  5 16:54:20 2017

@author: HeyDude
"""


import pydicom
import json
import warnings
from SimpleDicomToolkit.logger import Logger
import logging
import dateutil
from datetime import datetime, timedelta


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
        hdict = Encoder.encode(header)
        return Header.from_dict(hdict)


    def to_pydicom_header(self):
        return Decoder.dataset_from_dict(self)

class Encoder():
    """ Convert pydicom header to dictionary with sqlite3 compatible values """
    _PRIVATE_TAG_PREFIX = 'private_tag_'
    _PRIVATE_TAG_NAME = _PRIVATE_TAG_PREFIX + '{group}_{element}_{VR}_{VM}'

    DT_NULL = datetime(1800,1,1)
    TM_NULL = -1
    
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
            # try:                
            encoded = Encoder.encocode_element(element, skip_private_tags)
            
            if encoded is None:
                continue # skip element that failed encoding
            else:
                name, value = encoded

            dicom_dict[name] = value

            # convert dictionary to header object to enable . indexing of dict
            dicom_dict = Header.from_dict(dicom_dict)

        return dicom_dict

    @staticmethod
    def encocode_element(element, skip_private_tags=False):
        """ Return encoded name and encoded value for a pydicom element """
        name = Encoder._encode_tagname(element)
        if isinstance(element.value, pydicom.sequence.Sequence):
            value = Encoder._encode_sequence(element, skip_private_tags)
        else:
            try:
                value = Encoder._encode_value(element, skip_private_tags)
            except ValueError:
                warnings.warn('Cannot encode {0}, ommitting tag'.format(element),
                              RuntimeWarning)
                return None
        return name, value

    @staticmethod
    def encode_value_with_tagname(tagname, value):
        _, VR, VM = Decoder._decode_tagname(tagname)
        
        return Encoder._convert_value(value, VR)

    @staticmethod
    def _encode_tagname(element):
        # Get a valid name for sqlite3 column name. Private tags are encoded
        # in a specific format. Public tags are encoded as the pydicom keyword

        if element.keyword == '':
            # private or unknown tag
            name = Encoder._PRIVATE_TAG_NAME
            name = name.format(group = str(hex(element.tag.group)),
                               element = str(hex(element.tag.element)),
                               VR = element.VR,
                               VM = element.VM)
        else:
            name = element.keyword
        return name

    @staticmethod
    def _encode_sequence(seqs, skip_private_tags):
        
            # iterate over sequences each element of a sequence is a dataset
            values = []
            for seq in seqs:
                values += [Encoder.encode(seq, skip_private_tags)]
            value = json.dumps(values)
            return value

    @staticmethod
    def _encode_value(element, skip_private_tags):
        # encode the value of a pydicom element to a json string
        
        vr = element.VR
        vm = element.VM
        if Encoder.is_multiple(element.VM):
            # convert to list before converting elements in list
            
            value = json.dumps([Encoder._convert_value(vi, VR=vr, VM=vm)\
                                for vi in element.value])
       
        else:
            value = Encoder._convert_value(element.value, VR=vr, VM=vm)
        return value
    
    @staticmethod
    def _convert_value(value, VR='', VM='1'):
        if isinstance(value, pydicom.valuerep.PersonName3):
            # special treatment of person names
            value = json.dumps(value.original_string)
        elif VR == 'DA':
                value = Encoder.unix_time(value)
        elif VR == 'DT':
            value = Encoder.unix_time(value)
        elif VR == 'TM':
            if value == '':
                return Encoder.TM_NULL
            elif '.' in value:
                value = datetime.strptime(value, '%H%M%S.%f')
            else:
                value = datetime.strptime(value, '%H%M%S')
            epoch = datetime.utcfromtimestamp(0)
            value = datetime.combine(epoch.date(), value.time())
            value = Encoder.unix_time_millis(value)
        elif VR == 'AT':
            # dicom tag reference
            value = json.dumps(value.real)
        elif isinstance(value, bytes):
            # bytes are converted to hex string
            value = json.dumps(value.hex())
        else:
            # Convert also ints and floats to string, stored as text in 
            # database for simplicity. All columns are text except dates.
            value = json.dumps(value)
     
        return value
    
    @staticmethod
    def is_multiple(VM):
        if isinstance(VM, int):
            return True if VM > 1 else False
        elif isinstance(VM, str):
            return True if ('-' in VM or float(VM) > 1) else False
        
    @staticmethod
    def unix_time(dt):
        if isinstance(dt, str):
            if '.' in dt:
                dt = dt.split('.')[0]
            if dt == '':
                dt = Encoder.DT_NULL
            else:
                dt = dateutil.parser.parse(dt)
        epoch = datetime.utcfromtimestamp(0)
        delta = dt - epoch
        return delta.total_seconds()
    
    @staticmethod
    def unix_time_millis(dt):
        ms = 0
        if isinstance(dt, str) and '.' in dt:
            ms = int(dt.split('.')[1])
            dt = dt.split('.')[0]
        return int(Encoder.unix_time(dt) * 1000) + ms
    
class Decoder():
    """ Set of functions to convert sqlite3 compatible dicts to pydicom header
    """

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
    def decode(header_dict):
        """ Convert dictionary to pydicom dataset. """
        ds = pydicom.Dataset()
        for tagname, repval in header_dict.items():
            try:
                value, tag, vr, vm = Decoder.decode_entry(tagname, repval)
            except:
                raise ValueError('Cannot decode tag: {0}'.format(tagname))
            try:               
                ds.add_new(tag, vr, value)
            except:
                warnings.warn('Cannot add tag {0} with value {1} and VR {2}'\
                              .format(tag, value, vr), RuntimeWarning)

            if  ds[tag].VR == 'US or SS' and isinstance(value, int):
                ds[tag].VR = 'US' # force int to prevent invalid header

        # additional tags needed to write dataset to disk
        ds.is_little_endian = True
        ds.is_implicit_VR=False
        return ds
    
    
           
    @staticmethod
    def decode_entry(tagname, value):
        tag, vr, vm = Decoder._decode_tagname(tagname)
        
        if Decoder.is_sequence(tagname):
            # encoded sequence, recursive call
            return [Decoder.decode(vi) for vi in json.loads(value)], tag, vr, vm
        
        
        if Decoder.is_multiple(value):
            value = json.loads(value)
            value = [Decoder._decode_value(vi, VR=vr, VM=vm) for vi in value]
        else:
            value = Decoder._decode_value(value, VR=vr, VM=vm)
        
        if isinstance(value, str) and value == '-1.$': # HACK
            value = 0
        return value, tag, vr, vm

    @staticmethod
    def _decode_tagname(tagname):        
        if tagname in Decoder().dictionary_tag.keys():
            tag = Decoder().dictionary_tag[tagname]
            vr = pydicom.datadict.dictionary_VR(tag)
            vm = pydicom.datadict.dictionary_VM(tag)
        else:
            tagname = tagname.replace(Encoder._PRIVATE_TAG_PREFIX, '')
            group, elem, vr, vm = tagname.split(sep='_')
            tag = pydicom.tag.Tag(group, elem)
        return tag, vr, vm

    @staticmethod
    def _decode_value(dictvalue, VR = None, VM='1'):
        if dictvalue is None:
            value = None
        elif isinstance(dictvalue, str):
            value = json.loads(dictvalue)
        elif isinstance(dictvalue, (float, int)):
            value = dictvalue
        elif dictvalue is None:
            value=dictvalue
        else:
            msg = 'Value should be str, int or float. Not {0}'
            raise TypeError(msg.format(str(type(dictvalue))))
        
        if value is None:
            pass
        elif isinstance(value, list) and VR == 'SQ':
            return [Decoder.decode(vi) for vi in value]
        elif VR == 'OB':
            # bytes are stored as hex string, this should retrun bytes
            value = bytearray.fromhex(value).decode()
        elif VR == 'AT' and value is not None:
            # pydicom tags are stored as the real value          
            value = pydicom.tag.Tag(value)
        elif VR == 'US or SS':
            value=str(value)
        elif VR in ('DA', 'DT', 'TM'):
            if isinstance(dictvalue, str):
                dictvalue = json.loads(dictvalue)
            if isinstance(dictvalue, list):
                return [Decoder._decode_value(vi, VR=VR) for vi in dictvalue]
            elif VR == 'DA':
                value = (datetime(1970,1,1) + timedelta(seconds=dictvalue)).strftime('%Y%m%d')
            elif VR == 'DT':
                value = (datetime(1970,1,1) + timedelta(seconds=dictvalue)).strftime('%Y%m%d %H%M%S.%f')
            elif VR == 'TM':
                if value == Encoder.TM_NULL:
                    value = ''
                else:
                    value = (datetime(1970,1,1) + timedelta(seconds=dictvalue/1000))
                    value = value.strftime('%H%M%S.%f')
            if value == Encoder.DT_NULL:
                value = ''
        return value
    
    @staticmethod
    def is_sequence(tagname):
        _, VR, _ = Decoder._decode_tagname(tagname)
        return VR == 'SQ'
    
    @staticmethod
    def is_multiple(converted_value):
        try:
            value = json.loads(converted_value)
            if isinstance(value, list):
                return True
            else:
                return False
        except:
            return False
if __name__ == "__main__":
    file = 'C:\\Users\\757021\\Data\\Y90\\6772044\\WB\\1.3.12.2.1107.5.6.1.69069.30190118052910055366800000002'
    file = 'C:\\Users\\757021\\Data\\DAQSPECT\\Rotterdam\\dcm153959899.0000.dcm'
    header = pydicom.read_file(file)
    encoded = Encoder.encode(header)
    decoded = Decoder.decode(encoded)
    
    