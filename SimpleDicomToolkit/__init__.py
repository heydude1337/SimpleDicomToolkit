import yaml as _yaml
import pkg_resources as _pkg_resources
_resource_package = __name__
# resource_package = 'SimpleDicomToolkit'

_ALLOWED_KEYS = ['DEFAULT_FOLDER']
DEFAULT_FOLDER = 'default_folder'
_DEFAULT_FILE = _pkg_resources.resource_filename(_resource_package, 'defaults.yml')
_DEFAULTS = _yaml.load(open(_DEFAULT_FILE, 'r'))


def get_setting(setting, default_value = None):
    """
    Return a default configuration setting for the SimpleDicomToolit package.
    """

    return _DEFAULTS.get(setting, default_value)

def set_setting(setting, value):
    _DEFAULTS[setting] = value
    _yaml.dump(open(_DEFAULT_FILE, 'r'), _DEFAULTS)



from SimpleDicomToolkit.constants import *
from SimpleDicomToolkit.dicom_tags import *
from SimpleDicomToolkit.logger import Logger
from SimpleDicomToolkit.DicomDatabaseSQL import Database
from SimpleDicomToolkit.images_on_disk import ImagesOnDisk, CacheToDisk
from SimpleDicomToolkit.orthanc import OrthancUploader


