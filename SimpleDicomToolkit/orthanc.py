

# Orthanc - A Lightweight, RESTful DICOM Store
# Copyright (C) 2012-2016 Sebastien Jodogne, Medical Physics
# Department, University Hospital of Liege, Belgium
# Copyright (C) 2017-2018 Osimis S.A., Belgium
#
# This program is free software: you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.


import os
import os.path
import httplib2
import base64

"""
Sample script to recursively import in Orthanc all the DICOM files
that are stored in some path. Please make sure that Orthanc is running
before starting this script. The files are uploaded through the REST
API.

Usage: %s [hostname] [HTTP port] [path]
Usage: %s [hostname] [HTTP port] [path] [username] [password]
For instance: %s 127.0.0.1 8042 .
#"""

class OrthancUploader():
    def __init__(self, host = '127.0.0.1', port = 8042, username = None,
                 password = None):

        self.url = 'http://' + host + ':' + str(port) + '/instances'
        if username is not None and password is not None:
            encoded = base64.b64encode(self.username + ':' + self.password)
            self.authorization = 'Basic ' + encoded
        else:
            self.authorization = None
    @property
    def headers(self):
        headers = {'content-type': 'application/dicom'}
        if self.authorization is not None:
            headers['authorization'] = self.authorization
        return headers

    def upload_file(self, file):
        print('Upload file:' + file)
        f = open(file, "rb")
        content = f.read()
        f.close()
        h = httplib2.Http()

        try:
            resp, content = h.request(self.url, 'POST',
                                      body = content,
                                      headers = self.headers)
        except:
            msg = "=> unable to connect (Is Orthanc running? Is there a password?)"
            print(msg)

        if resp.status == 200:
            print(" => success\n")

        else:
            print(" => failure (Is it a DICOM file? Is there a password?)\n")

    def upload_folder(self, folder):
        print('Uploading folder: ' + folder)
        for root, dirs, files in os.walk(folder):
            for f in files:
                self.upload_file(os.path.join(root, f))
        print('Finished!')

if __name__ == "__main__":
    folder = '/Users/marcel/Python/PSMA/temp/0202-0370'
    uploader = OrthancUploader(host = 'proliant', port = 8042)
    uploader.upload_folder(folder)

