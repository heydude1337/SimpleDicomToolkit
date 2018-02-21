import os
import SimpleITK as sitk
import pickle
from os.path import splitext

class CacheToDisk(dict):
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
        self.read_only = read_only
        
        self.update(*args, **kwargs)
    
    def __getitem__(self, key):
        
        try:
            data = super().__getitem__(key)
        except KeyError:
            file_name = self._file_name_for_key(key)
            
            try:
                data = self._load(file_name)
            except:
                print('Cannot read: {0}'.format(file_name))
                raise 
        return data
    
    def __setitem__(self, key, value):   
        super().__setitem__(key, value)
        if not(self.read_only): 
            try:
                self._dump(self._file_name_for_key(key), value)
            except:
                print('Data dump failed for {0}'.format(key))
                raise
    def _file_name_for_key(self, key):
        return os.path.join(self.folder, key) 
       
    def _load(file_name):
        return pickle.load(open(file_name, 'rb'))
    def _dump(file_name, data):
        
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
                
    def _load(self,file_name):
        return sitk.ReadImage(file_name)

    def _dump(self, file_name, image):   
        # force nifti
        file, ext = splitext(file_name)
        ext = '.nii'
        file_name = file + ext
        try:
            sitk.WriteImage(image, file_name)
        except:
            print('Cannot write {0}'.format(file_name))
            raise 
            