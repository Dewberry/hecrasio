"""
PFRA Module for working with HEC-RAS model files
"""

import pathlib as pl
import zipfile
import io
import geopandas as gpd
import pandas as pd
from io import BytesIO
import rasterio
import gdal
gdal.UseExceptions()

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

class RasModel(object):
    '''
    This object holds information for the files stored in a hec-ras zip file used for STARRII PFRA study.
    If path starts with s3 then the code will run from s3 zipfile, otherwise path is expected
    to be a string path to a zipped local model (e.g. *.zip)
    '''
    def __init__(self, path:str, s3_data:bool=True, zipped:bool=True, verbose:bool=False):
        assert 'zip' in path, "Model files must be stored in a .zip file"

        def getS3Zip(self):
            '''Returns zipfile data from s3'''
            path_parts = pl.PurePosixPath(self.s3path).parts
            bucket = path_parts[1]
            key = '/'.join(path_parts[2:])
            obj = s3.Object(bucket_name=bucket, key=key)
            buffer = io.BytesIO(obj.get()["Body"].read())
            return zipfile.ZipFile(buffer)

        self.s3path      = path
        self.name        = str(pl.PurePosixPath(self.s3path).name).replace('.zip','')
        self._modelType  = self.name.split('_')[1][0]
        self._subType    = self.name.split('_')[2]
        self._zipfile    = getS3Zip(self)
        self._contents   = [x.filename for x in self._zipfile.infolist()]

        try:
            self.prj_file = [x for x in self._contents if '{}.prj'.format(self.name) in x][0]
        except:
            print('No prj file found')

    @property
    def zipfile(self):
        return self._zipfile

    @property
    def subType(self):
        return self._subType

    @property
    def modelType(self):
        modelType  = self._modelType
        assert modelType =='F' or modelType == 'P', 'Check Model Nomenclature, expected a P or F Model, found {}'.format(modelType)
        if self._modelType =='F':
            return 'Fluvial'

        elif self._modelType =='P':
            return 'Pluvial'

    @property
    def contents(self):
        return self._contents

    
class PointData:
    def __init__(self, shapefile:str, fields:list =['plus_code']) -> bool:
        self.required_fields = fields
        self._shapefile  = shapefile
        self._shapefile_path  = pl.PurePosixPath(self._shapefile)

        def get_geodataframe(self):
            return gpd.read_file(self._shapefile)

        self.geodataframe = get_geodataframe(self)

        def check_fields(self):
            '''Check required fields'''
            missing_fields = [f for f in self.required_fields if f not in self.geodataframe.columns]
            assert len(missing_fields) < 1, "Required fields not found in shapefile: {}".format(str(missing_fields)) 

        self._field_check = check_fields(self)

        def get_projection(self):
            '''return projection'''
            return self.geodataframe.crs
            pass

        self._current_projection = get_projection(self)
        
    @property
    def projection_string(self):
        gdf_crs = rasterio.crs.CRS.from_dict(self._current_projection).to_proj4()
        return rasterio.crs.CRS.from_string(gdf_crs)
    
class GridObject:

    def __init__(self, tiff:str):
        self._tiff          = tiff
        self._posix_path    = pl.PurePosixPath(self._tiff)
        self._tiff_name     = self._posix_path.name
        
        def is_local(self):
            if self._posix_path.parts[0]=='s3:':
                return False
            else:
                return True
            
        self._is_local = is_local(self)
            
        def read_from_s3(self) -> 'gdal objects':
            assert not self._is_local, 'Tiff must be on s3 to use this function'
            s3 = boto3.resource('s3')
            s3Obj = s3.Object(self._bucket, self._prefix)
            image_data = BytesIO(s3Obj.get()['Body'].read())
            tif_inmem = "/vsimem/data.tif" #Virtual Folder to Store Data
            gdal.FileFromMemBuffer(tif_inmem, image_data.read())
            src = gdal.Open(tif_inmem)  
            return src.GetRasterBand(1), src.GetGeoTransform(), src
        
        def read_from_local(self) -> 'gdal objects':
            src = gdal.Open(self._tiff)  
            return src.GetRasterBand(1), src.GetGeoTransform(), src
            
        if self._is_local:
            self._bucket, self._prefix = None, None
            self._rasterBand, self._geoTrans, self._src = read_from_local(self)
            
        else:
            self._bucket = self._posix_path.parts[1]
            self._prefix = '/'.join(self._posix_path.parts[2:]) 
            self._rasterBand, self._geoTrans, self._src = read_from_s3(self)
            
    @property
    def posix_path(self):
        return self._posix_path

    @property
    def tiff_name(self):
        return self._tiff_name
    
    @property
    def rb(self):
        return self._rasterBand
    
    @property
    def gt(self):
        return self._geoTrans
    
    @property
    def src(self):
        return self._src
    
    @property
    def no_data_value(self):
        return self._rasterBand.GetNoDataValue()
    
    @property
    def projection_string(self):
        try:
            tiff_crs = osr.SpatialReference(self._src.GetProjectionRef()).ExportToProj4()
            tiff_crs = rasterio.crs.CRS.from_proj4(tiff_crs).to_proj4()
            return rasterio.crs.CRS.from_string(tiff_crs)
        except:
            print('Check Tiff Coordinate System, osr unable to find projection data')
            return None
        
# Functions ---------------------------------------------------------------------
def s3List(bucketName, prefixName, nameSelector, fileformat):
    '''
        This function takes an S3 bucket name and prefix (flat directory path) and returns a list of GeoTiffs.
            This function utilizes boto3's continuation token to iterate over an unlimited number of records.
        
        BUCKETNAME -- A bucket on S3 containing GeoTiffs of interest
        PREFIXNAME -- A S3 prefix.
        NAMESELECTOR -- A string used for selecting specific files. E.g. 'SC' for SC_R_001.tif.
        FILEFORMAT -- A string variant of a file format.
    '''
    # Set the Boto3 client
    s3_client = boto3.client('s3')
    # Get a list of objects (keys) within a specific bucket and prefix on S3
    keys = s3_client.list_objects_v2(Bucket=bucketName, Prefix=prefixName)
    # Store keys in a list
    keysList = [keys]
    # While the boto3 returned objects contains a value of true for 'IsTruncated'
    while keys['IsTruncated'] is True:
        # Append to the list of keys
        # Note that this is a repeat of the above line with a contuation token
        keys = s3_client.list_objects_v2(Bucket=bucketName, Prefix=prefixName,
                                         ContinuationToken=keys['NextContinuationToken'])
        keysList.append(keys)
    
    # Create a list of GeoTiffs from the supplied keys
    #     While tif is hardcoded now, this could be easily changed.
    pathsList = []
    for key in keysList:
        paths = ['s3://'+ bucketName + '/' + elem['Key'] for elem in key['Contents'] \
                     if elem['Key'].find('{}'.format(nameSelector))>=0 and elem['Key'].endswith(fileformat)]
        pathsList = pathsList + paths
        
    return pathsList

def query_gdf(gdf: gpd.geodataframe, gt: any, rb: any, point_id:str) -> dict:
    """
    Return point: pixel value pair for a given row in geodataframe
    """
    results={}
    for idx in gdf.index:
        pointID = gdf[point_id].iloc[idx]
        x, y = gdf.iloc[idx].geometry.x, gdf.iloc[idx].geometry.y
        px = int((x-gt[0]) / gt[1])   
        py = int((y-gt[3]) / gt[5])  
        try:
            pixel_value = rb.ReadAsArray(px,py,1,1)[0][0]
            results[pointID] = pixel_value
        except TypeError as e:
            results[pointID] = 'Error, verify projection is correct and Point is whithini Tiff bounds'
            
    return results


def extract_values_at_points(points:gpd.geodataframe.GeoDataFrame, tiffs:list) -> pd.DataFrame:
    """Identifies raster values at a given series of points returning a DataFrame."""
    results={}

    for tif in tiffs:
        tif = GridObject(tif)
        results[tif.tiff_name.strip('.tif')] =  query_gdf(points, points.index, tif.gt, tif.rb)
        
    i=0
    for k, v in results.items():
        testd = [(list(d.keys())[0], list(d.values())[0]) for d in v]
        if i ==0:
            df = pd.DataFrame.from_records(testd).set_index(0).rename(columns={1:k})
            i+=1
        else:
            tmp = pd.DataFrame.from_records(testd).set_index(0).rename(columns={1:k})
            df = pd.concat([df,tmp], axis=1)
    
    return df