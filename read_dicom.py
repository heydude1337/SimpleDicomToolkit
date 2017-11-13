import SimpleDicomToolkit
import dateutil
import os
import SimpleITK as sitk




# source: http://dicom.nema.org/dicom/2013/output/chtml/part05/sect_6.2.html
VR_STRING   = ('PN', 'AE', 'AS', 'AT', 'CS', 'LO','LT','OB', \
               'SH', 'ST', 'UI', 'UN', 'UT')
VR_DATE     = 'DA'
VR_DATETIME = 'DT'
VR_TIME     = 'TM'
VR_FLOAT    = ('DS','FL', 'FD', 'OD') #DS is unparsed, FL and FD are floats
VR_INT      = ('IS', 'SL', 'UL', 'US')
VR_SEQ      = 'SQ'

class DicomReadable():
    """ Superclass for DicomFiles and DicomDatabaseSQL """
    _images = None
    _image = None
    MAX_FILES = 5000 # max number of files to be read at by property images
    SUV = True # convert PET images to SUV
    
    @property
    def files(self):
        # must be implemented by subclass
        raise NotImplementedError
    
    @property
    def series_count(self):
        """ Return the number of dicom series present"""
        if not(hasattr(self, 'SeriesInstanceUID')): return 0
        uids = getattr(self, 'SeriesInstanceUID')
        if type(uids) is str:
            return 1
        elif type(uids) is list:
            return len(uids)
        else:
            raise ValueError
    
    @property
    def image(self):
        """ Returns an sitk image for the files in the files property.
            All files must belong to the same dicom series 
            (same SeriesInstanceUID). """
            
        assert self.series_count == 1
        if self._image is None:
            try:
                self._image = read_serie(self, SUV = self.SUV)
            except:
                print('Error during reading image serie')
                raise
                
        return self._image
    
    @property
    def images(self):
        """ Returns a dictionary with keys the SeriesInstanceUID and
            values the sitkimage belonging tot the set of files belonging to 
            the same dicom series (same SeriesInstanceUID). Number of files
            in the files property cannot exceed the MAX_FILES property. 
            This prevents reading of too large data sets """
            
        if len(self.files) > self.max_files:
            print('Number of files exceeds MAX_FILES property')
            raise IOError
        
        if self._images is None:
            assert hasattr(self, SimpleDicomToolkit.SERIESINSTANCEUID)
            try:
                self._images = read_series(self, SUV = self.SUV)
            except:
                print('Error during reading image series')
                raise
                
        return self._images
    

def read_files(file_list):
    """ Read a file or list of files using SimpleTIK. A file list will be
         read as an image series in SimpleITK. """
    if type(file_list) is str:
      file_reader = sitk.ImageFileReader()      
      file_reader.SetFileName(file_list)

    elif type(file_list) in (tuple, list):
      file_reader = sitk.ImageSeriesReader()
      file_reader.SetFileNames(file_list)

    try:
        im = file_reader.Execute()
    except:
        print('cannot read file: {0}'.format(file_list))
        raise IOError

    return im


def read_series(dicom_files, series_uids = None,
                flatten = True, rescale = True, SUV = False):
    """ Read an entire dicom database to SimpleITK images. A dictionary is 
        returned with SeriesInstanceUID as key and SimpleITK images as values.
        
        series_uids: When None (default) all series are read. Otherwise a 
                    single SeriesInstanceUID may be specified or a list of UIDs
                    
        split_acquisitions: Returns seperate images for each acquisition number.
        single_output: Return a single image and header if only one dicom series
                       was found. Same output as read_serie.
        """
        
       
    if series_uids is None: # read everyting
        series_uids = dicom_files.SeriesInstanceUID

    if type(series_uids) not in (tuple, list):
        series_uids = [series_uids]
        
    dicom_filess = [dicom_files.filter(SimpleDicomToolkit.SERIESINSTANCEUID, uid) \
                    for uid in series_uids]
    
    reader = lambda df: read_serie(df, SUV = SUV, rescale=rescale)
    result = [reader(df) for df in dicom_filess]
    
    images, headers = list(zip(*result))
    
    if len(images) == 1 and flatten:
        images = images[0]
        headers = headers[0]
    
    return images
      
def read_serie(dicom_files, rescale = True, SUV = False):
    """ Read a single image serie from a dicom database to SimpleITK images. 
        
        series_uid: Define the SeriesInstanceUID to be read from the database.
                    When None (default) it is assumed that the database 
                    contains a single image series (otherwise an error 
                    is raised).
                                    
        split_acquisitions: Returns seperate images for each acquisition number.
        """

    
    assert dicom_files.series_count == 1 # multiple series should be read by read_series
    
    try: # sort slices may heavily depend on the exact dicom structure from the vendor. 
        # Siemens PET and CT have a slice location property
        dicom_files = dicom_files.sort('SliceLocation')
    except:
        print('Slice Sorting Failed')

    files = dicom_files.files
    if hasattr(dicom_files, 'folder'):
        files = [os.path.join(dicom_files.folder, file) for file in files]
    image = read_files(files)
    
    if rescale:
        slope, intercept = rescale_values(dicom_files)   
        
        #image *= slope
        #image += intercept
        
    
    # calculate and add a SUV scaling factor for PET.
    if dicom_files.SOPClassUID == SimpleDicomToolkit.SOP_CLASS_UID_PET:
        try:
            factor = suv_scale_factor(dicom_files)
            
        except:
            print('No SUV factor could be calculated!')
            factor = 1
            
        if SUV:
            image *= factor
        
        setattr(dicom_files, SimpleDicomToolkit.SUV_SCALE_FACTOR, factor) 
        
    return image

def suv_scale_factor(header):
    """ Calculate the SUV scaling factor (Bq/cc --> SUV) based on information
    in the header. Works on Siemens PET Dicom Headers. """
    
    # header = image.header
    # calc suv scaling
     
    nuclide_info   = header.RadiopharmaceuticalInformationSequence[0]
    nuclide_dose   = float(nuclide_info.RadionuclideTotalDose)
    injection_time = nuclide_info.RadiopharmaceuticalStartTime
    half_life      = float(nuclide_info.RadionuclideHalfLife)
    series_time    = header.SeriesTime
    patient_weight = float(header.PatientWeight)
    
    
    injection_time = dateutil.parser.parse(injection_time)
    series_time = dateutil.parser.parse(series_time)
    
    delta_time = (series_time - injection_time).total_seconds()
    
    decay_correction = 0.5 ** (delta_time / half_life)
      
    suv_scaling = (patient_weight * 1000) / (decay_correction * nuclide_dose) 
      
    return suv_scaling
  

def rescale_values(header = None):
    # apply rescale slope and intercept to the image
    
    if hasattr(header, SimpleDicomToolkit.REALWORLDVALUEMAPPINGSEQUENCE):
        slope = header.RealWorldValueMappingSequence[0].RealWorldValueSlope
    elif hasattr(header, SimpleDicomToolkit.RESCALESLOPE):
        slope = header.RescaleSlope
    else:
        print('No rescale slope found in dicom header')
        slope = 1
    
    if hasattr(header, SimpleDicomToolkit.REALWORLDVALUEMAPPINGSEQUENCE):
        intercept = header.RealWorldValueMappingSequence[0].RealWorldValueIntercept
    elif hasattr(header, SimpleDicomToolkit.RESCALEINTERCEPT):
        intercept = header.RescaleIntercept
    else:
        print('No rescale slope found in dicom header')
        intercept = 1
    
    
    return slope, intercept




