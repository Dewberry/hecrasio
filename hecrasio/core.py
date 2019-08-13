"""
PFRA Module for working with HEC-RAS model files
"""

import pathlib as pl
import zipfile
import io

try:
    import boto3

    resource = boto3.resource('s3')
    s3 = resource
except ModuleNotFoundError as e:
    print('Verify boto3 is installed and credentials are stored, {}'.format(e))


class ResultsZip:
    """
    HEC-RAS Model Data
    Files (currently) must be read in from a .zip file.
    PFRA set to false if not a StarII study
    """

    def __init__(self, path: str, require_prj: bool = True, pfra: bool = True):
        assert 'zip' in path, "Model files must be stored in a .zip file"
        self._abspath = path
        self._pure_path = pl.Path(path)
        self._pfra = pfra

        def get_s3_zip():
            """
            If path starts with s3 then the code will run from s3 zipfile, otherwise path is expected
            to be a string path to a zipped local model (e.g. *.zip)
            """
            obj = s3.Object(bucket_name=self._pure_path.parts[1],
                            key='/'.join(self._pure_path.parts[2:])
                            )
            buffer = io.BytesIO(obj.get()["Body"].read())
            return zipfile.ZipFile(buffer)

        if 's3' in self._abspath:
            self._cloud_platform = 'aws'
            self._zipfile = get_s3_zip()

        elif 'gs' in self._abspath:
            """Placeholder to method for google"""
            self._cloud_platform = 'google'

        elif 'azure' in self._abspath:
            """Placeholder to method/attribute for azure"""
            pass

        else:
            self._cloud_platform = None

        self._contents = [x.filename for x in self._zipfile.infolist()]

        # Check Nomenclature rules for STARR II PFRA products
        if self._pfra:
            assert '_out' in self._pure_path.stem, "Expected '_out.zip'"
            self._name = self._pure_path.stem.replace('_out', '')
            try:
                self._modelType = self._name.split('_')[1][0]
                self._subType = self._name.split('_')[2]
            except IndexError as e:
                print("File format not consistentent with PFRA studies.\n Set PFRA to false.")

        else:
            self._name = self._pure_path.stem
            self._modelType = None
            self._subType = None

        # If project file is required (placeholder if update-info implemented)
        if require_prj:
            try:
                self.prj_file = [x for x in self._contents if '{}.prj'.format(self._name) in x][0]
            except IndexError as prjError:
                print('No prj file found, {}'.format(prjError))

    @property
    def zipfile(self):
        """Add Description
        """
        return self._zipfile

    @property
    def subType(self):
        """Add Description
        """
        return self._subType

    @property
    def modelType(self):
        """Add Description
        """
        if self._pfra:
            assert self._modelType == 'F' or self._modelType == 'P', 'Check Model Nomenclature, expected a P or F ' \
                                                                     'Model, found {}'.format(self._modelType)

            if self._modelType == 'F':
                return 'Fluvial'

            elif self._modelType == 'P':
                return 'Pluvial'
        else:
            # Placeholder to add modelTypes where needed
            return 'Other'

    @property
    def contents(self):
        """Add Description
        """
        return self._contents
