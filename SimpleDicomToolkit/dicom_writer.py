#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 21 11:26:30 2018

@author: marcel
"""

import SimplePhantomToolkit as sptk
import SimpleITK as sitk
import numpy as np
import datetime
import yaml
import os
import pkg_resources


resource_package = 'SimpleDicomToolkit'


import pydicom
from pydicom import uid as UID
from pydicom.dataset import Dataset, FileDataset
from pydicom.sequence import Sequence
# from pydicom.tag import Tag



NBITS = 16
# reference to this script:
MEDIASTORAGESOPINSTANCEUID = '1.2.752.37.3135516787.1.20180201.121933.1'

# not sure about this field but this value works for PET and SPECT
IMPLEMENTATIONCLASSUID = '1.2.752.37.5.4.15'

PET_SOP_CLASS = '1.2.840.10008.5.1.4.1.1.20'
NM_SPECT_SOP_CLASS = '1.2.840.10008.5.1.4.1.1.20'
#NM_SPECT_ACQ_SOP_CLASS = '1.2.840.10008.5.1.4.1.1.20'

def _clean_file_dataset(SOPClassUID = None):
    file_meta = Dataset()
    file_meta.MediaStorageSOPClassUID = SOPClassUID
    file_meta.MediaStorageSOPInstanceUID = MEDIASTORAGESOPINSTANCEUID
    file_meta.ImplementationClassUID = IMPLEMENTATIONCLASSUID
    file_meta.ImplementationVersionName = 'Python Generated'
    file_meta.FileMetaInformationVersion = b'\x00\x01'
    file_meta.TransferSyntaxUID = UID.ImplicitVRLittleEndian
    ds = FileDataset('dummy.dcm', {},
                     file_meta=file_meta, preamble=b"\0" * 128)
    return ds

def sitk_to_nm_dicom(sitk_image=None, template=None):
    # CONSTANTS
    NBITS = 16
    ATTRIBUTE_FILE = 'nm_sop_class.yml'

    # scale image
    rescale_slope, sitk_image = image_to_int(sitk_image)

    #Empty DataSet
    ds = _clean_file_dataset(SOPClassUID = NM_SPECT_SOP_CLASS)

    #Set Default attributes
    seq_fcns = mapping_sequence_functions()
    ds = set_default_attributes(ds, template=template, sop_file=ATTRIBUTE_FILE,
                    seq_fcns = seq_fcns[NM_SPECT_SOP_CLASS])

    ds = set_image_attributes(ds, sitk_image)

    ds.RealWorldValueMappingSequence[0].RealWorldValueSlope = rescale_slope

    ds = set_bit_information(ds, nbits = NBITS)

    set_image_data(ds, sitk_image)

    # special attribute refers to other tag
    # setattr(ds,'FrameIncrementPointer', Tag(0x54, 0x80))

    return ds

def sitk_to_pet_dicom(sitk_image=None, template=None):
    rescale_slope, sitk_image = image_to_int(sitk_image)

    # UID's to be used for all slices
    UIDS = {}
    UIDS['SeriesInstanceUID'] = UID.generate_uid()

    if template:
        UIDS['StudyInstanceUID'] = template.StudyInstanceUID
        UIDS['FrameOfReferenceUID'] = template.FrameOfReferenceUID
    else:
        UIDS['StudyInstanceUID'] = UID.generate_uid()
        UIDS['FrameOfReferenceUID'] = UID.generate_uid()

    slice_data = get_slice_information(sitk_image)

    dss = [] # gather all datasets for all slices
    for index, location, thickness, image in slice_data:
        ds = _create_pet_slice(index, location, thickness, image,
                               rescale_slope = rescale_slope,
                               UIDS = UIDS,
                               template = template)
        dss += [ds] # append data set

    return dss

#def sitk_to_nm_acq_dicom(sitk_image=None, template=None):
#    NBITS = 16
#    ATTRIBUTE_FILE = 'nm_spect_acq_sop_class.yml'
#
#    ds = _clean_file_dataset(SOPClassUID = NM_SPECT_ACQ_SOP_CLASS)
#    ds = set_default_attributes(ds, template=template, sop_file=ATTRIBUTE_FILE,
#                    seq_fcns = mapping_sequence_functions()[NM_SPECT_SOP_CLASS])
#    if template is None:
#        ds.DetectorInformationSequence = [_detector_information_sequence(),
#                                          _detector_information_sequence()]

def _create_pet_slice(slice_index, slice_location, slice_thickness, slice_image,
                      template=None, rescale_slope = 1, UIDS = {}):

    ATTRIBUTE_FILE = 'pet_sop_class.yml'

    ds = _clean_file_dataset(SOPClassUID = PET_SOP_CLASS)

    ds = set_default_attributes(ds, template=template, sop_file = ATTRIBUTE_FILE,
                    seq_fcns = mapping_sequence_functions()[PET_SOP_CLASS])

    ds = set_image_attributes(ds, slice_image)

    setattr(ds, 'RescaleSlope', rescale_slope)
    setattr(ds, 'SliceLocation', slice_location)
    setattr(ds, 'SliceThickness', slice_thickness)

    for tag, value in UIDS.items():
        setattr(ds, tag, value)

    x, y = slice_image.TransformIndexToPhysicalPoint((0,0))
    setattr(ds, 'ImagePositionPatient', [x,y,slice_location])
    setattr(ds, 'ImageIndex', slice_index)

    ds = set_image_data(ds, slice_image)

    ds = set_bit_information(ds, nbits = NBITS)

    return ds

def get_slice_information(sitk_image):

    locations = []
    for i in range(0, sitk_image.GetSize()[2]):
        locations += [sitk_image.TransformIndexToPhysicalPoint((0,0, i))[2]]

    sitk_slices = [sitk_image[:,:,i] for i in range(0, len(locations))]

    slice_indices = list(range(0, len(sitk_slices)))

    slice_thickness = [sitk_image.GetSpacing()[2]] * len(slice_indices)
    return tuple(zip(slice_indices, locations, slice_thickness, sitk_slices))


def image_to_int(sitk_image, nbits=16):
    # force float first
    sitk_image = sitk.Cast(sitk_image, sitk.sitkFloat64)

    # obtain maximum voxel value
    max_val = sptk.geometry.sitk_max(sitk_image)

    # scale image between 0 and 2^nbits -1
    sitk_image = sitk_image / max_val * (2**nbits-1)

    # make integer
    sitk_image = sitk.Cast(sitk_image, sitk.sitkUInt16)

    # calculate rescale slope
    rescale_slope = max_val / (2**nbits-1)

    return rescale_slope, sitk_image


def set_image_data(ds, sitk_image):
    pixel_array = sitk.GetArrayFromImage(sitk_image)
    setattr(ds, 'PixelData', pixel_array.tostring(order=None))
    return ds

def set_default_attributes(ds, template=None, sop_file = None, seq_fcns = {}):
    REPLACE_DATE = '<Today Date>'
    REPLACE_TIME = '<Today Time>'
    NEW_UID = '<New UID>'
    REMOVE = '<None>'
    NEW_SEQUENCE = '<Generate Sequence>'

    TODAY_TIME = datetime.datetime.now().time().strftime('%H%M%S')
    TODAY_DATE = str(datetime.date.today()).replace('-','')

    sop_file_full = pkg_resources.resource_filename(resource_package, sop_file)
    attributes = yaml.load(open(sop_file_full, 'r'))
    for attribute, (copy, value) in attributes.items():
        if value == REPLACE_DATE:
            value = TODAY_DATE
        if value == REPLACE_TIME:
            value = TODAY_TIME
        if value == NEW_UID:
            value = UID.generate_uid()
        if value == NEW_SEQUENCE:
            value = seq_fcns[attribute]()
        if copy and template:
            if hasattr(template, attribute):
                setattr(ds, attribute, getattr(template, attribute))
            elif REMOVE != value:
                setattr(ds, attribute, value)
        elif REMOVE != value:
            setattr(ds, attribute, value)

    return ds

def set_image_attributes(ds, sitk_image):
    tags = [tag.keyword for tag in ds]

    # spacing and slice thickness
    if sitk_image.GetDimension() == 3:
        px, py, pz = sitk_image.GetSpacing()
        nx, ny, nslices = sitk_image.GetSize()
        setattr(ds, 'SliceThickness', pz)
        setattr(ds, 'SpacingBetweenSlices', pz)
        setattr(ds, 'NumberOfSlices', nslices)
    elif sitk_image.GetDimension() == 2:
        px, py = sitk_image.GetSpacing()
        nx, ny = sitk_image.GetSize()

    # rows, columns and slices
    setattr(ds, 'Rows', nx)
    setattr(ds, 'Columns', ny)
    setattr(ds, 'PixelSpacing', [px, py])

    if 'NumberOfFrames' in tags:
        setattr(ds, 'NumberOfFrames', ds.NumberOfSlices)
    if 'SliceVector' in tags:
        vector =  np.arange(1, ds.NumberOfSlices+1).tolist()
        setattr(ds, 'SliceVector', vector)

    min_pixel = int(round(sptk.geometry.sitk_min(sitk_image)))
    max_pixel = int(round(sptk.geometry.sitk_max(sitk_image)))

    if 'LargestImagePixelValue' in ds:
        byte_val = (max_pixel).to_bytes(2, byteorder='little')
        setattr(ds, 'LargestImagePixelValue', byte_val)
    if 'SmallestImagePixelValue' in ds:
        byte_val
        setattr(ds, 'SmallestImagePixelValue', byte_val)

    window_center = (max_pixel - min_pixel) / 2
    window_width = max_pixel - min_pixel

    setattr(ds, 'WindowCenter', window_center)
    setattr(ds, 'WindowWidth', window_width)

    return ds


def set_bit_information(ds, nbits = NBITS):
    setattr(ds, 'BitsAllocated', NBITS)
    setattr(ds, 'BitsStored', NBITS)
    setattr(ds, 'HighBit', NBITS-1)
    return ds

def _detector_information_sequence(RadialPosition = 'None'):
    seq = Dataset()
    seq.CenterofRotationOffset = '0'
    seq.FieldofViewShape = 'RECTANGLE'
    seq.FieldofViewDimension = ['537', '383']
    seq.FocalDistance = '0'
    seq.CollimatorGridName = '@18887197'
    seq.CollimatorType = 'PARA'
    seq.ImageOrientationPatient = [1, 0, 0, 0, 1, 0]
    seq.ImagePositionPatient = [0, 0, 0]
    seq.XFocusCenter = ['0', '0']
    seq.YFocusCenter = ['0', '0']
    seq.ZoomFactor = ['1', '1']
    seq.StartAngle = '180'
    view_seq = Dataset()
    view_seq.CodeValue = 'G-A117'
    view_seq.CodingSchemeDesignator = 'SRT'
    view_seq.CodeMeaning = 'Transverse'
    view_seq.ViewModifierCodeSequence = Sequence()

    seq.ViewCodeSequence = Sequence([view_seq])

    if RadialPosition is not 'None':
        seq.RadialPosition = RadialPosition
    return Sequence([seq])

def _radiopharmaceutical_information_sequence():
    seq = Dataset()
    seq.Radiopharmaceutical = 'None'
    seq.RadiopharmaceuticalRoute = ''
    seq.RadiopharmaceuticalVolume = ''
    seq.RadiopharmaceuticalStartTime = '000000'
    seq.RadionuclideTotalDose = 0
    seq.RadionuclideHalfLife = '0'
    nuclide_seq = Dataset()
    nuclide_seq.CodeValue = ''# 'C-163A8'
    nuclide_seq.CodingSchemeDesignator = '' #'99SDM'
    nuclide_seq.CodeMeaning = '' #'Technetium Tc-99m'
    seq.RadionuclideCodeSequence = Sequence([nuclide_seq])
    pharm_seq = Dataset()
    pharm_seq.CodeValue = ''
    pharm_seq.CodingSchemeDesignator  = ''
    pharm_seq.CodeMeaning = ''
    seq.RadiopharmaceuticalCodeSequence = Sequence([pharm_seq])
    return Sequence([seq])

def _real_world_value_mapping_sequence(nbits=NBITS):
    units_seq = Dataset()
    units_seq.CodeMeaning = 'Bq/ml'
    units_seq.CodeValue = 'Bq/ml'
    units_seq.CodingSchemeDesignator = 'UCUM'

    real_world_seq = Dataset()
    real_world_seq.MeasurementUnitsCodeSequence = Sequence([units_seq])

    minval = (0).to_bytes(length=2, byteorder='little')
    maxval = (2**nbits-1).to_bytes(length=2, byteorder='little')
    real_world_seq.RealWorldValueFirstValueMapped = minval
    real_world_seq.RealWorldValueLastValueMapped = maxval
    real_world_seq.RealWorldValueSlope = 1
    real_world_seq.RealWorldValueIntercept = 0

    return Sequence([real_world_seq])

def mapping_sequence_functions():
    nm_sequence_mapping_fcn = {
        'DetectorInformationSequence': _detector_information_sequence,
        'RealWorldValueMappingSequence': _real_world_value_mapping_sequence,
        'RadiopharmaceuticalInformationSequence': _radiopharmaceutical_information_sequence}


    pet_sequence_mapping_fcn = {
        'RadiopharmaceuticalInformationSequence': _radiopharmaceutical_information_sequence}
#    nm_acq_sequence_mapping_fcn = {
#        'RadiopharmaceuticalInformationSequence': _radiopharmaceutical_information_sequence}
    fcns = {PET_SOP_CLASS: pet_sequence_mapping_fcn,
            NM_SPECT_SOP_CLASS: nm_sequence_mapping_fcn}
            #NM_SPECT_ACQ_SOP_CLASS: nm_acq_sequence_mapping_fcn}

    return fcns

def write(dicom_data, export_folder = './output', file_name = 'dicom_file.dcm',
          SeriesDescription = None):
    if not(isinstance(dicom_data, (tuple, list))):
        dicom_data = [dicom_data]

    os.makedirs(export_folder, exist_ok = True)

    for image_number, data_set in enumerate(dicom_data):
        if SeriesDescription:
            data_set.SeriesDescription = SeriesDescription
        file_name_part, ext = os.path.splitext(file_name)
        file_name_part += '_{0}'.format(image_number)
        full_file_name = os.path.join(export_folder, file_name_part) + ext
        data_set.save_as(full_file_name)
    return True

if __name__ == "__main__":
    from DicomDatabaseSQL import Database

    db = Database('/Users/marcel/Horos Data/DATABASE.noindex')
    data = db.query(PatientID = '1530707', SeriesDescription = 'SPECT 24h pi hals/thorax')
    image = data.image
    file = data.files[0]
    dcm_data = pydicom.read_file(file)

    names = sorted([pydicom.datadict.dictionary_keyword(tag) for tag in dcm_data.keys() if not(tag.is_private)])
    items = yaml.load(open('nm_spect_acq_sop_class.yml'))
    item_names = sorted(list(items.keys()))
    to_remove = [name for name in item_names if name not in names]
    to_add = [name for name in names if name not in item_names]
#    import dose_map
#    import SimpleDicomToolkit as sdtk
#    pet_dosemap = dose_map.pet_dose_map(yaml_file = '1043509.yml')
#    dss_clean_pet = sitk_to_pet_dicom(sitk_image=pet_dosemap)
#
#    data = dose_map.load_data('1043509.yml')
#    db = sdtk.Database(folder = data[dose_map.DICOM_FOLDER])
#    pet = db.query(**{**data[dose_map.PET], **{sdtk.PATIENTID: data[sdtk.PATIENTID]}})
#    template = dicom.read_file(os.path.join(data[dose_map.DICOM_FOLDER], pet.files[0]))
#
#    dss_template_pet = sitk_to_pet_dicom(sitk_image=pet_dosemap, template=template)
#    write(dss_template_pet, export_folder='./template_pet',
#          SeriesDescription = 'PET_DOSE_MAP')
#    spect_dosemap = dose_map.spect_dose_map(yaml_file = '1043509.yml')
#
#    dss_clean_spect = sitk_to_nm_dicom(sitk_image=spect_dosemap)
#    write(dss_clean_spect , export_folder='./clean_spect',
#          SeriesDescription = 'SPECT_DOSE_MAP')
#
#    spect = db.query(**{**data[dose_map.SPECT], **{sdtk.PATIENTID: data[sdtk.PATIENTID]}})
#    template = dicom.read_file(os.path.join(data[dose_map.DICOM_FOLDER], spect.files[0]))
#
#    dss_template_spect = sitk_to_nm_dicom(sitk_image=spect_dosemap, template=template)
#    write(dss_template_spect , export_folder='./template_spect',
#          SeriesDescription = 'SPECT_DOSE_MAP')