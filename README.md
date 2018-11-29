# SimpleDicomToolkit
Builds a sqlite3 database for all dicom files in a folder. Dicom files can
be quickly found by searching dicom fields. Finally (some) images and image
volumes can be read directly. SimpleDicomToolkit provides a clean pythonic
interface for handling a large collection of dicom files

## Usage

build or load database:

```python
db = SimpleDicomToolkit(path='/mydicomfolder')
```

database have to be build only once. The database is saved to a file and
automatically loaded next time. SimpleDicomToolkit will search every time
for new folders within the specified path and add them to the database. Removed
files from the path will be deleted from the database as well on subsequent
loading.

query the database:

```python
db = db.select(PatientName='MyPatient', StudyDescription='MyStudy')
```

This will select all dicom files that match above specified PatientName and
Study Name. Any valid dicom filed can be used to query/select the files.

```python
db.files and db.files_with_fullpath
```

Will give a list of currently selected files from the database with the relative
and fullpath to these files. If no selection is made it will give a list of
all files.

```python
db.selection
```

Will return the currently used filter for selecting the files.

```python
db.reset()
```

Will remove the current selection filter

```python
db.SeriesDescription
```

Will give a list of (unique) SeriesDescriptions for the current selection. This
works for all dicom fields (PatientName, StudyDescription, etc.)

## Reading images

```python
myscript(db.files_with_path)
```

You can use your own script to read the dicom files by passing the filenames
of the current selection.

```python
db.image
```

In addition after selecting a single dicom series, this may return a SimpleITK a
image. It probably works for CT, PET, SPECT, and planar imaging and might
work for MRI.

```python
db.array
```

Will return a numpy array for the given selection.

## Advanced usage

```python
db = SimpleDicomToolkit(path='/mydicomfolder', scan=False)
```
Will load a currently stored database, but will not scan for new files.

```python
db = SimpleDicomToolkit(path='/mydicomfolder', force_rebuild=True)
```

Will remove existing database and rebuild the database from scratch

```python
db = SimpleDicomToolkit(path='/mydicomfolder', in_memory=True)
```

Do not create a database file, but only create a temporary database in memory.
Database will not be saved.

```python
db = SimpleDicomToolkit(path='/myfolder', SUV=True)
```

Will convert images to SUV when using db.image and db.array. This works probably
for Siemens PET and may or may not work for other vendors due to possible different
dicom implementations of SUV values.

```python
db.reset('SeriesDescription')
```

Will only remove the specified dicom field from the current selection.

## Limitations

Small databases up to 10GB should take a couple of minutes to build and can
be accessed within seconds after build. Database up to 100GB work quite well, but
performance on Windows is much better than linux/macos. sqlite3 is better
optimized for Windows it seems. Due to very poor (none at all) database design,
databases over 100GB can be very slow to access. SimpleDicomToolkit is primarily
intended to be used for relatively small projects.





