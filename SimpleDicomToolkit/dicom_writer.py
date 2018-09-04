import pydicom
import yaml
import datetime
import SimpleITK as sitk
import collections
import os
from copy import copy
from sitktools.tools import min, max
import SimpleDicomToolkit as sdtk

# list of all public dicom tags as keywords
_dicom_dict = pydicom.datadict.DicomDictionary.values()
DICOM_TAGS = [value[-1] for value in _dicom_dict if value[-1]]
DICOM_TAGS += ['Isotope'] # hack to make radionuclide sequence working
ALL = 'all'

class Module(collections.MutableMapping):
    """ Base class for dicom modules """
    
    # flag to determine if values can be copied from a template for this module
    COPY = False 
    
    __empty_dataset = None
    def __init__(self, image=None, **kwargs):
        self.image = image
        self.mapping = dict()
        
        for key, value in kwargs.items():
            self[key] = value   
            
        for attr in dir(self):
            if attr in DICOM_TAGS and (attr not in self.keys()):
       
                self[attr] = getattr(self, attr)
    
    @classmethod
    def tags(cls):
        """ Return the tags defined for this module """
        tags = []
        for attr in dir(cls):
            if attr in DICOM_TAGS:
                tags += [attr]
        return tags
    
    @property
    def _empty_dataset(self):
        # Should return an 'empty' data set to which tags and values are added
        # To write dicom files this should return a pydicom FileDataSet
        if self.__empty_dataset is None:
            return pydicom.Dataset()
        else:
            return copy(self.__empty_dataset)
    
    @_empty_dataset.setter
    def _empty_dataset(self, dataset):
        self.__empty_dataset = dataset
        
        
    def __getitem__(self, key):
        # Some values, like UIDS, need to be generated. Mapping may contain
        # functions that return a value when called. 
        value = self.mapping[key]
        if callable(value):
            value = value()
        return value

    def __setitem__(self, key, value):
        if key not in DICOM_TAGS:
            raise KeyError('Key must be a dicom tag name!')
        self.mapping[key] = value
           
    def __delitem__(self, key):
        del self.mapping[key]
    
    def __iter__(self):
        return iter(self.mapping)
    
    def __len__(self):
        return len(self.mapping)
    
    def keys(self):
        return self.mapping.keys()
    
    def _get_dataset(self):
        # Create a pydicom dataset for this module.
        dataset = self._empty_dataset
        for attr, value in self.items():
            try:
                setattr(dataset, attr, value)
            except:
                msg = 'Cannot set {1} on object {0} with value {2}'
                raise RuntimeError(msg.format(self.__class__.__name__, 
                                              attr, str(value)))
        return dataset

    @property
    def dataset(self):
        """ Returns a pydicom dataset for this module """
        return self._get_dataset()
    
    def copy_from_dataset(self, dataset):
        for tagname in self.keys():
            try:
                value = getattr(dataset, tagname)
            except AttributeError:
                value = None
            if value is not None:
                self[tagname] = value



class InstanceModule(Module):
    """ Module that contains values that are instance (slice) specific, 
    such as ImagePositionPatient. The property index determines wich instance
    (slice) values to return. """
    def __init__(self, index=None, **kwargs):
        super().__init__(**kwargs)
        self.index = index
    
    def _get_dataset(self, index=None):
        if index is not None:
            self.index=index
        
        dataset = super()._get_dataset()
        
        return dataset


class ImagePlaneModule(InstanceModule):
    COPY = False
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
  
    def PixelSpacing(self):
        return list(self.image.GetSpacing()[0:2])
    
    def ImageOrientationPatient(self):
         return list(self.image.GetDirection())
    
    def ImagePositionPatient(self):
        pos = self.image.TransformIndexToPhysicalPoint((0, 0, self.index))
        return list(pos)
            
class SingleSequenceModule(Module):
    def _get_dataset(self):
        dataset = super()._get_dataset()
        return pydicom.Sequence([dataset])
        
class RadiopharmaceuticalInformationSequenceModule(SingleSequenceModule):
    _code_table = None
    Isotope = 'F-18'
    RadiopharmaceuticalStartDateTime = ''
    RadionuclideTotalDose = 0
    
    @property
    def code_table(self):
        if self._code_table is None:
            data = yaml.load(open('radionuclides.yml', 'r'))
            self._code_table = data['RadionuclideHalflifes']
        return self._code_table
    
    @property
    def RadionuclideHalfLife(self):
        value = self.code_table[self.Isotope]
        mult = {'s': 1, 'm': 60, 'h': 3600, 'd': 24*3600, 'y': 365*24*3600}
        value = float(value[:-2]) * mult[value[-1]]
        return value
    
    @property
    def RadionuclideCodeSequence(self):
        return RadionuclideCodeSequenceModule(Isotope=self.Isotope).dataset
    
class RadionuclideCodeSequenceModule(SingleSequenceModule):
    _code_table = None
    Isotope = 'F-18'
    
    @property
    def code_table(self):
        if self._code_table is None:
            data = yaml.load(open('radionuclides.yml', 'r'))
            self._code_table = data['RadionuclideCodeSequence']
        return self._code_table
    @property
    def CodeValue(self):
        return self.code_table[self.Isotope]['CodeValue']
    @property
    def CodingSchemeDesignator(self):
        return self.code_table[self.Isotope]['CodingSchemeDesignator' ]
    @property
    def CodeMeaning(self):
        return self.code_table[self.Isotope]['CodeMeaning']
    
    
class PETIsotopeModule(Module):
    COPY = True
    Isotope = 'F-18'
    RadiopharmaceuticalStartDateTime = ''
    RadionuclideTotalDose = 0
    _RadiopharmaceuticalInformationSequence=None
    @property
    def RadiopharmaceuticalInformationSequence(self):
        # return a pydicom dataset with the nuclide information
        if self._RadiopharmaceuticalInformationSequence:
            return self.EmptyRadiopharmaceuticalInformationSequence()
        else:
            return self._RadiopharmaceuticalInformationSequence
    
    @RadiopharmaceuticalInformationSequence.setter
    def RadiopharmaceuticalInformationSequence(self, value):
        self._RadiopharmaceuticalInformationSequence = value
        print(value)
        
    def EmptyRadiopharmaceuticalInformationSequence(self):
        mod = RadiopharmaceuticalInformationSequenceModule
        tagnames = PETIsotopeModule.tags()
        tagnames.remove('RadiopharmaceuticalInformationSequence')
        mapping = dict([(tag, getattr(self, tag)) for tag in tagnames])
        module = mod(**mapping)
        return module.dataset
        
class GeneralImageModule(InstanceModule):        
    COPY = True
    def InstanceNumber(self):
        return self.index
  
class ImagePixelModule(InstanceModule):
    COPY = False
    SamplesPerPixel = 1
    PhotometricInterpretation = 'MONOCHROME2'
    PixelRepresentation = 0
    BitsAllocated = 16
    BitsStored = 16
    HighBit = 16 - 1
    
    def Rows(self):
        return self.image.GetSize()[0]  
    
    def Columns(self):
        return self.image.GetSize()[1]    
    
    def PixelData(self):
        pixel_array = sitk.GetArrayFromImage(self.image[:, :, self.index])
        return pixel_array.tostring(order=None)

class ImageModule(Module):
    COPY = True
    RescaleSlope = 0
    RescaleIntercept = 1

class PETImageModule(ImageModule):
    COPY = True
    FrameReferenceTime = 0
    AcquisitionDate = ''
    AcquisitionTime = ''
    ActualFrameDuration = 1000
    DecayFactor = 1
    ImageIndex = 0

class CTImageModule(ImageModule):
    COPY = True
    ImageType = ['ORIGINAL', 'PRIMARY', 'AXIAL']
    RescaleType = 'HU'
    KVP = ''
    AcquisitionNumber = 1
    SeriesDate = lambda _: datetime.datetime.now().date().strftime('%Y%m%d')

class GeneralEquipmentModule(Module):
    COPY = True
    Manufacturer = ''

class FrameOfReferenceModule(Module):
    COPY = False
    FrameOfReferenceUID = lambda _: pydicom.uid.generate_uid()
    PositionReferenceIndicator = ''

class GeneralSeriesModule(Module):
    COPY = False
    Modality = '' 
    SeriesInstanceUID = lambda _: pydicom.uid.generate_uid()
    SeriesNumber = None
    PatientPosition = 'HFS'
    SeriesDescription = 'No Description'

class SOPCommonModule(Module):
    COPY = False
    SOPCLassUID = None
    SOPInstanceUID = lambda _: pydicom.uid.generate_uid()
    
class GeneralStudyModule(Module):
    COPY = True
    StudyInstanceUID = lambda _: pydicom.uid.generate_uid()
    StudyDate = ''
    StudyTime = ''
    ReferringPhysicianName = ''
    StudyID = ''
    AccessionNumber = ''
            
class PatientModule(Module):
    COPY = True
    PatientName = 'Unknown Patient' 
    PatientID = '123456'
    PatientSex = 'O'

class PETSeriesModule(Module):
    COPY = True
    SeriesDate = lambda _: datetime.datetime.now().date().strftime('%Y%m%d')
    SeriesTime = lambda _: datetime.datetime.now().time().strftime('%H%M%S.%f')
    Units = 'BQML'
    CountsSource = 'Emmission'
    SeriesType = ['WHOLE BODY', 'IMAGE']

    CorrectedImage = ['NORM', 'DTIM', 'ATTN', 'SCAT', 'DECY', 'RAN']
    DecayCorrection = 'START'
    CollimatorType = 'NONE'
    
    def NumberOfSlice(self):
        return self.image.GetSize()[2]

class FileMetaDataModule(Module):
    MediaStoragSOPClassUID = '1'    
    MediaStorageSOPInstanceUID = '1.2.752.37.3135516787.1.20180201.121933.1'
    ImplementationClassUID = '1.2.752.37.5.4.15'
    ImplementationVersionName = 'Python Generated'
    FileMetaInformationVersion = b'\x00\x01'
    TransferSyntaxUID = pydicom.uid.ImplicitVRLittleEndian
    
    def __init__(self, *args, SOPClassUID=None, **kwargs):
        super().__init__(*args, **kwargs)
        if SOPClassUID:
            self.MediaStorageSOPClassUID = SOPClassUID

    def _get_dataset(self):
        file_meta = super()._get_dataset()
        ds = pydicom.FileDataset('dummy.dcm', {}, file_meta=file_meta,
                                 preamble=b"\0" * 128)
        return ds
    
def image_to_int(image, nbits=16, intercept=None, slope=None):
    """ Calculate rescale slope and intercept and convert image to
        integer with the specified amount of bits. """
    if intercept and min(image-intercept) < 0:
        msg = ('Image Values would be negative with intercept: {0}.'
               'a new intercept will be calculated')
        print(msg.format(intercept))
        intercept=None
    cast = {8: sitk.sitkUInt8,
            16: sitk.sitkUInt16,
            32: sitk.sitkUInt32,
            64: sitk.sitkUInt64}

    if nbits not in cast.keys():
        raise ValueError('Invalid number of bits: {0}'.format(nbits))

    image = sitk.Cast(image, sitk.sitkFloat64)

    if not intercept:
        intercept = min(image)

    if min(image - intercept) < 0:
#            logger.debug('Intercept: %s', intercept)
        raise ValueError('Values must be > 0 after rescaling')

    max_val = max(image - intercept)

    if slope is None:
        slope =  max_val / (2**(nbits)-1)

    image = (image - intercept) / slope

    # check values, use round to circumvent precision errors
    if round(max(image)) > (2**nbits-1): 
        raise OverflowError('Maximum image value exceeds bit size')

    # make integer
    image = sitk.Cast(image, cast[nbits])

    return slope, intercept, image

class SOPStorageClass():
    modules = () # Modules that are used to convert to dicom
    DEFAULTBITS = 16 # Used when BitsAllocated is not present
    
    filename = 'dummy.dcm'
    folder = './test'
    
    _module = None
    _image = None
    
    def __init__(self, image, folder=None, filename=None, template=None, 
                 **kwargs):
        self.image = image
        
        if filename:
            self.filename=filename
            
        if folder:
            self.folder = folder
        
        if GeneralStudyModule in self.modules:
            self.StudyInstanceUID = pydicom.uid.generate_uid()
        
        if GeneralSeriesModule in self.modules:
            self.SeriesInstanceUID = pydicom.uid.generate_uid()
            
        if FrameOfReferenceModule in self.modules:
            self.FrameOfReferenceUID = pydicom.uid.generate_uid()
            
        if template:
            self.use_template(template)
            
        # overrides tags in the modules
        for key, value in kwargs.items():
            if key in DICOM_TAGS:
                print(key, value)
                setattr(self, key, value)
            else:
                raise KeyError
        
    def _tags_to_copy(self):
         to_copy = []
         for module in self.modules:
            if module.COPY:
                to_copy += module.tags()
         return to_copy
    
    def use_template(self, template=None):
        """ Copy values from template """
        if isinstance(template, str):
            template = pydicom.read_file(template)
            
        for tag in self._tags_to_copy():
            if hasattr(template, tag):                
                setattr(self, tag, getattr(template,tag))
    @property
    def image(self):
        return self._image
    
    @property
    def file_dataset(self):
        return FileMetaDataModule(SOPClassUID = self.SOPClassUID).dataset
    
    @property
    def module(self):
        if self._module is None:
            # generate a subclass from all selected modules
            module_cls = type('ComposedModule', self.modules, {})
            self._module = module_cls()
            self._module.image = self.image
            # datasets need to be derived from the pydicom FileDataSet
            self._module._empty_dataset = self.file_dataset

            for name in dir(self):
                # copy tags to module (override module defaults!)
                if name in DICOM_TAGS:
                    self._module[name] = getattr(self, name)
        return self._module
                              
    @image.setter
    def image(self, image):
        self._set_image(image)
       
    def write(self):
        os.makedirs(self.folder, exist_ok=True)
        pydicom.write_file(self.filename, self.dataset)
        
    def _set_image(self, image):
        self._image = image
        self._module = None
        
    def __setattr__(self, name, value):
        if name in DICOM_TAGS:
            # override default values in module
            if name in self.module.keys():
                self.module[name] = value
        super(SOPStorageClass, self).__setattr__(name, value)

    @property
    def dataset(self):
        return self._get_dataset()
    
    def _get_dataset(self):       
        return self.module.dataset
        
class ImagePlaneSeriesStorage(SOPStorageClass):
    """ Superclass for SOPs that implement the ImagePlaneModule """
    DEFAULT_BITS = 16
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
    
    def _set_image(self, image, intercept=None):
        nbits = getattr(self, 'BitsStored', self.DEFAULT_BITS)
        
        slope, intercept, image = image_to_int(image, nbits=nbits,
                                               intercept=intercept)
        self.RescaleSlope = slope
        self.RescaleIntercept = intercept        
        super()._set_image(image)
    
    def _tags_to_copy(self):
        to_copy = super()._tags_to_copy()
        
        for x in ('RescaleIntercept', 'RescaleSlope'):
            to_copy.remove(x)
        
        return to_copy
    
    def datasets(self, index=0):
        dataset = self.module._get_dataset(index=index)
        return dataset
    
    def write(self):
        file, ext = os.path.splitext(self.filename)
        folder = self.folder
        os.makedirs(folder, exist_ok=True)
        sdtk.progress_bar(0, self.image.GetSize()[2]-1, 'Writing ')
        for i in range(self.image.GetSize()[2]):
            sdtk.progress_bar(i, self.image.GetSize()[2]-1, 'Writing ')
            fullfile = os.path.join(folder, file+'_'+ str(i) + ext)
            pydicom.write_file(fullfile, self.datasets(index=i))
            
class PETImageStorage(ImagePlaneSeriesStorage):
    SOPClassUID = '1.2.840.10008.5.1.4.1.1.128'
    modules = (SOPCommonModule,
               PatientModule,
               GeneralStudyModule,
               GeneralSeriesModule,
               PETSeriesModule,
               FrameOfReferenceModule,
               GeneralEquipmentModule,                                          
               PETImageModule,
               ImagePlaneModule,
               GeneralImageModule,
               ImagePixelModule,
               PETIsotopeModule)
               
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def _set_image(self, image):
        super()._set_image(image, intercept=0)

class CTImageStorage(ImagePlaneSeriesStorage):
    SOPClassUID = '1.2.840.10008.5.1.4.1.1.2'
    modules = (SOPCommonModule,
               PatientModule,
               GeneralStudyModule,
               GeneralSeriesModule,
               FrameOfReferenceModule,
               GeneralEquipmentModule,                                          
               ImagePlaneModule,
               GeneralImageModule,
               ImagePixelModule,
               CTImageModule)
               
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def _set_image(self, image):
        super()._set_image(image, intercept=-1024)

def write_pet(image, folder='./dicom', filename='pet.dcm', template=None,
              **kwargs):
    sop = PETImageStorage(image=image, folder=folder, filename=filename,
                          template=template, **kwargs)
    sop.write()
    return sop

def write_ct(image, folder='./dicom', filename='ct.dcm', template=None,
              **kwargs):
    sop = CTImageStorage(image=image, folder=folder, filename=filename,
                          template=template, **kwargs)
    sop.write()   
    
if __name__ == "__main__":
    import SimpleDicomToolkit as sdtk
    import numpy as np
    import matplotlib.pyplot as plt
#    try:
#       image
#    except:
    db = sdtk.Database('F:/PSMA/DICOM', scan=False)
#        image = db.reset().select(PatientName='0202-0378', 
#                        SeriesDescription='LD_CT_WB_3/3').image
    folder = 'H:/share/2. Personeel/Marcel/test'
#    #folder = './test'
#    sop = CTImageStorage(image=image, folder=folder)
#    sop.use_template(db.files_with_path[0])
#    sop.write()
    pet = db.reset().select(PatientName='0202-0378', 
                        SeriesDescription='PET_WB_AC_EARL').image
    petsop = PETImageStorage(image=pet, folder=folder + '/pet',
                             template=db.files_with_path[0])
    petsop.write()
    
                     
