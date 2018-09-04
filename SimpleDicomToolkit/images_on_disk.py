import os
import SimpleITK as sitk
import pickle
from os.path import splitext
from SimpleDicomToolkit import Logger
import logging
class CacheToDisk(dict, Logger):
    _LOG_LEVEL = logging.DEBUG
    def __init__(self, folder = None, read_only = False,
                 *args, **kwargs):
        """ Dictionary that keeps a pickle dump of data inside a disk folder.
            Data are the values of the dictionary, filenames are keys of
            the the dictionary. If the key is not present, the folder will be
            searched for that file and the file will be read from disk. If a
            value is set for a specific key, this value will be written to
            disk with the key as filename

            folder:     Folder where the image willes will be written/read.
                        By default the current working directory is used.
            read_only:  Do not write to disk, images will be retrieved from
                        diks, but will not be written to disk. Default = False.

        """

        super().__init__()

        if folder is None:
            folder = os.getcwd()

        if not(os.path.exists(folder)):
            try:
                os.makedirs(folder, exist_ok = True)
            except:
                print('Cannot create folder: {0}'.format(folder))
        self.folder = folder
        self.logger.debug('Folder: %s', self.folder)

        self.read_only = read_only
        self._load_existing()
        self.update(*args, **kwargs)
        self.logger.debug('%s images found in folder', str(len(self.keys())))


    def _load_existing(self):
        # make existing files visible in keys
        for file in os.listdir(self.folder):
            if os.path.isfile(file):
                super().__setitem__(file, file) # set value to str do not load

    def __getitem__(self, key):

        try:
            data = super().__getitem__(key)
            if isinstance(data, str):
                raise KeyError # force loading of file
        except KeyError:
            file_name = self._file_name_for_key(key)
            if os.path.isdir(file_name):
                data = self.__class__(folder=file_name,
                                      read_only=self.read_only)
            else:
                try:
                    data = self._load(file_name)
                except:
                    raise IOError('Cannot read: {0}'.format(file_name))
        return data

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        # dump to disk if value is not string. Strings are set during
        # init for existing files.
        if not(self.read_only) and not isinstance(value, str):
            try:
                self._dump(self._file_name_for_key(key), value)
            except:
                raise IOError('Data dump failed for {0}'.format(key))

    def _file_name_for_key(self, key):
        return os.path.join(self.folder, key)

    def _load(self, file_name):
        return pickle.load(open(file_name, 'rb'))

    def _dump(self, file_name, data):
        pickle.dump(open(file_name, 'rb'), data)

class ImagesOnDisk(CacheToDisk):

    """ Dictionary that keeps a copy of sitk images inside a disk folder.
        Images are stored in the values of the disk. Filenames are keys of
        the the dictionary. If the key is not present, the folder will be
        searched for that file and the file will be read from disk. If a
        value is set for a specific key, this value will be written to
        disk with the key as filename

        folder:     Folder where the image willes will be written/read.
                    By default the current working directory is used.
        read_only:  Do not write to disk, images will be retrieved from
                    diks, but will not be written to disk. Default = False.

    """
    def _load_existing(self):
        # make existing nifti files visible in keys
        for file in os.listdir(self.folder):
            if os.path.isfile(os.path.join(self.folder, file)):
                file, ext = os.path.splitext(file)
                if ext.lower() == '.nii':
                    # set value to str do not load, loading will be done
                    # when value for key is requested
                    super().__setitem__(file + ext, file + ext)

    def _load(self,file_name):
        return sitk.ReadImage(file_name)

    def _dump(self, file_name, image):
        # force nifti
        file, ext = splitext(file_name)
        ext = '.nii'
        file_name = file + ext
        self.logger.debug('Writing: ' + file_name)
        try:
            sitk.WriteImage(image, file_name)
        except:
            raise IOError('Cannot write {0}'.format(file_name))

if __name__ == "__main__":
    folder = 'F:/PSMA/NII/0207-0116'
    images = ImagesOnDisk(folder)