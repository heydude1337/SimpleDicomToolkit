"""
Created on Tue Sep  5 16:54:20 2017

@author: HeyDude
"""
import os

import SimpleDicomToolkit
import SimpleITK as sitk
import dateutil
import pydicom
import warnings

def sitk_image(path):
    """ Get SITK image from dicom file(s) containing a single dicom series.
    Path may be a folder, file or list of files """
    return SimpleDicomToolkit.Database(path, in_memory=True).image

def sitk_images(path):
    """ Get SITK images from dicom file(s) containing one or more dicom series.
    Path may be a folder, file or list of files. A dictionary is returned with
    keys the SeriesInstanceUID and has sitk images as values. """
    return SimpleDicomToolkit.Database(path, in_memory=True).images

def numpy_array(path):
    """ Get numpy array from dicom file(s) containing a single dicom series.
    Path may be a folder, file or list of files """
    return SimpleDicomToolkit.Database(path, in_memory=True).array

def numpy_arrays(path):
    """ Get numpy arrays from dicom file(s) containing one or more dicom series.
    Path may be a folder, file or list of files. A dictionary is returned with
    keys the SeriesInstanceUID and has numpy arrays as values. """
    return SimpleDicomToolkit.Database(path, in_memory=True).arrays


def read_files(file_list):
    """ Read a file or list of files using SimpleTIK. A file list will be
         read as an image series in SimpleITK. """
    if len(file_list) == 1:
        file_list = file_list[0]
    if isinstance(file_list, str):
        file_reader = sitk.ImageFileReader()
        file_reader.SetFileName(file_list)

    elif isinstance(file_list, (tuple, list)):
        file_reader = sitk.ImageSeriesReader()
        file_reader.SetFileNames(file_list)

    try:
        image = file_reader.Execute()
    except:
        raise IOError('cannot read file: {0}'.format(file_list))


    return image


def read_serie(files, rescale=True, SUV=False, folder=None, SUVparams={}):
    """
    Read a single image serie from a dicom database to SimpleITK images.
    """

    if folder is not None:
        files = [os.path.join(folder, file) for file in files]

    image = read_files(files)
    image = sitk.Cast(image, sitk.sitkFloat64)
    slope, intercept = rescale_values(pydicom.read_file(files[0],
                                                    stop_before_pixels=True))

    image *= slope
    image += intercept

    # calculate and add a SUV scaling factor for PET.
    if SUV:
        factor = suv_scale_factor(pydicom.read_file(files[0],
                                                    stop_before_pixels=True),
                                  SUVparams)
        image *= factor
        image.BQML_TO_SUV = factor
        image.SUV_TO_BQML = 1/factor

    return image

def suv_scale_factor(header, SUVparams={}):
    """ Calculate the SUV scaling factor (Bq/cc --> SUV) based on information
    in the header. Works on Siemens PET Dicom Headers. """

    # header = image.header
    # calc suv scaling
    nuclide_info   = header.RadiopharmaceuticalInformationSequence[0]

    parse = lambda x: dateutil.parser.parse(x)
    series_datetime_str = header.SeriesDate + ' ' + header.SeriesTime
    series_dt = parse(series_datetime_str)

    injection_time = nuclide_info.RadiopharmaceuticalStartTime

    injection_datetime_str = header.SeriesDate + ' ' + injection_time
    injection_dt = parse(injection_datetime_str)

    nuclide_dose   = float(getattr(nuclide_info,'RadionuclideTotalDose', -1))
    patient_weight = float(getattr(header, 'PatientWeight', -1))
    halflife      = float(getattr(nuclide_info, 'RadionuclideHalfLife', -1))

    # enable override of SUV parameters
    if isinstance(SUVparams, dict):
        patient_weight = SUVparams.pop('patient_weight', patient_weight)
        halflife = SUVparams.pop('patient_weight', halflife)
        nuclide_dose = SUVparams.pop('nuclide_dose', nuclide_dose)
        injection_dt = SUVparams.pop('injection_dt', injection_dt)

    delta_time = (series_dt - injection_dt).total_seconds()
    decay_correction = 0.5 ** (delta_time / halflife)
    suv_scaling = (patient_weight * 1000) / (decay_correction * nuclide_dose)

    return suv_scaling


def rescale_values(header=None):
    """ Return rescale slope and intercept if they are in the dicom headers,
    otherwise 1 is returned for slope and 0 for intercept. """
    # apply rescale slope and intercept to the image

    if hasattr(header, SimpleDicomToolkit.REALWORLDVALUEMAPPINGSEQUENCE):
        slope = header.RealWorldValueMappingSequence[0].RealWorldValueSlope
    elif hasattr(header, SimpleDicomToolkit.RESCALESLOPE):
        slope = 1# sitk does rescaling header.RescaleSlope
    else:
        warnings.warn('\nNo rescale slope found in dicom header\n',
                      RuntimeWarning)
        slope = 1

    if hasattr(header, SimpleDicomToolkit.REALWORLDVALUEMAPPINGSEQUENCE):
        intercept = header.RealWorldValueMappingSequence[0].RealWorldValueIntercept
    elif hasattr(header, SimpleDicomToolkit.RESCALEINTERCEPT):
        intercept = 0 # sitk does rescaling header.RescaleIntercept
    else:
        warnings.warn('\nNo rescale slope found in dicom header\n',
                      RuntimeWarning)
        intercept = 0

    return slope, intercept

