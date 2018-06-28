import os


class FileScanner():

    @staticmethod
    def compare(files, existing_files):
        # use sets for performance
        files = set(files)
        existing_files = set(existing_files)

        # files that are in database but were not found in file folder
        not_found = list(existing_files.difference(files))


        # files in path but not in database
        new_files = list(files.difference(existing_files))


        return new_files, not_found

    @staticmethod
    def recursive_generator(path):
        """Recursively yield DirEntry objects for given directory."""
        for entry in os.scandir(path):
            if entry.is_dir(follow_symlinks=False):
                yield from FileScanner.recursive_generator(entry.path)
            else:
                yield entry

    @staticmethod
    def files_in_folder(folder, recursive=False, absolute_path=False):
        if recursive:
            file_gen = FileScanner.recursive_generator(folder)
        else:
            file_gen = os.scandir(folder)
        if absolute_path:
            files = [os.path.normpath(entry.path) for entry in file_gen]
        else:
            files = [os.path.relpath(file, folder) for file in file_gen]
        return files

if __name__ == "__main__":
    folder = 'C:\\Users\\757021\\Data\\'
    files = FileScanner.files_in_folder(folder, recursive=True)