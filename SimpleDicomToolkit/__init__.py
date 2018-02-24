from SimpleDicomToolkit.constants import *
from SimpleDicomToolkit.dicom_tags import *
from SimpleDicomToolkit.logger import Logger
from SimpleDicomToolkit.DicomDatabaseSQL import Database
from SimpleDicomToolkit.images_on_disk import ImagesOnDisk, CacheToDisk
from SimpleDicomToolkit.orthanc import OrthancUploader
import yaml as _yaml

_defaults_file = 'defaults.yml'
_DEFAULTS = _yaml.load(open(_defaults_file, 'r'))
_ALLOWED_KEYS = ['DEFAULT_FOLDER']
DEFAULT_FOLDER = 'default_folder'

def get_setting(setting, default_value = None):
    """ 
    Return a default configuration setting for the SimpleDicomToolit package.
    """
    return _DEFAULTS.get(setting, default_value)

def set_setting(setting, value):
    _DEFAULTS[setting] = value
    _yaml.dump(open(_defaults_file, 'r'), _DEFAULTS)

