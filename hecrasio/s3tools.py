import pathlib as pl
import boto3
from io import BytesIO
import zipfile
import os
import osr
import rasterio
import sys 
import os
import subprocess
import pathlib as pl
import papermill as pm
import boto3 
from datetime import datetime
from glob import glob
import logging
import boto3
from botocore.exceptions import ClientError


OUTPUT_EXTS = ['.html', '.ipynb', '.csv', '.tif', '.vrt']

def get_model_paths(model_id:str)-> tuple:
    study_area = model_id.split('_')[0]
    model_type = model_id.split('_')[1]
    sub_type = model_id.split('_')[2]
    event_id = model_id.split('_')[3]
    
    # Zipped model contents
    model_output = "s3://pfra/{0}/{1}/{2}/{3}/{0}_{1}_{2}_{3}_out.zip".format(study_area, model_type, sub_type, event_id)
    
    # All Pluvial Models built from H00 input
    if sub_type in ['H06', 'H12', 'H24', 'H96']:
        subtype = 'H00'
        
    model_input = "s3://pfra/{0}/BaseModels/{0}_{1}_{2}.zip".format(study_area, model_type, sub_type)
    point_data =  "s3://pfra/RiskAssessment/{0}/Points/{0}_{1}_Points.zip".format(study_area, model_type)
    output_dir = model_output[:-25]
    return model_input, model_output, point_data, output_dir


def write_rasmap_file(projection:str, planFile:str, terrain:str) -> any:
    """Write xml required for rasmapper to compute results from hdf, given a terrain"""
    xmldata =  """<RASMapper>\n  <RASProjectionFilename Filename="{0}" />
      <Results>
        <Layer Type="RASResults" Filename="{1}">
          <Layer Name="elevation" Type="RASResultsMap">
          <MapParameters MapType="elevation" OutputMode="Stored Current Terrain" Terrain="{2}" ProfileIndex="2147483647" />
          </Layer>
        </Layer>
      </Results>
      <Terrains>
        <Layer Name="{2}" Type="TerrainLayer" Filename="{2}.hdf">
          <ResampleMethod>near</ResampleMethod>
        </Layer>
      </Terrains>
    <RenderMode>hybrid</RenderMode>
    <MarksWarpMethod>False</MarksWarpMethod>\n</RASMapper>""".format(projection, planFile, terrain.split('.')[0])
    return xmldata

def check_map_created(pipe_text: any) -> any:
    if pipe_text.split('\r\n')[-2] !='Completed storing 1 results map layer':
        print(pipe_text)
    else:
        return True

def get_point_from_s3(s3_data_path:str) -> None:
    """Download model specific point data"""
    path_info =pl.Path(s3_data_path.split('//')[1])
    s3 = boto3.resource('s3')
    s3Obj = s3.Object(path_info.parts[0], '/'.join(path_info.parts[1:]))
    buffer = BytesIO(s3Obj.get()['Body'].read())
    inmem_zip = zipfile.ZipFile(buffer)

    for file in inmem_zip.infolist():
        inmem_zip.extract(file)
    return None

def get_terrain_data(terrainDir:str, s3_model_input:str, projection = 'Projection')-> None:
    """Download zip containing model input, extract terrain & projection datasets"""
    model = RasModel(s3_model_input)
    terrrain_files = [f for f in model.contents if 'Terrain' in f and f.split('.')[-1] in ['hdf', 'tif', 'vrt']]
    projection_files = [f for f in model.contents if 'Projection' in f and f.split('.')[-1] in ['prj']]

    for f in terrrain_files:
        idx = model.zipfile.namelist().index(f)
        tmp = '{}'.format(pl.Path(f).name)
        model.zipfile.filelist[idx].filename = tmp
        model.zipfile.extract(model.zipfile.filelist[idx])
        if '.tif' in f:
            terrain = tmp
    
    assert len(projection_files) == 1, 'Too many projection files found []'.format(projection_files)
    projection_file =  projection_files[0]
    
    idx = model.zipfile.namelist().index(projection_file)
    projection_file_name = '{}_PROJECTION.{}'.format(model.name, 'prj')
    model.zipfile.filelist[idx].filename = projection_file_name
    model.zipfile.extract(model.zipfile.filelist[idx])
    del model
    return None

def collect_output_data(jobID: str) -> str:
    """Move and rename output from RASMapper"""
    ras_grid_files = glob(os.path.join(os.getcwd(), '**', 'WSE*'), recursive=True)
    rasGrid, rasVRT = ras_grid_files[0], ras_grid_files[1]
    rasGridRename = 'WSE_{}.{}'.format(jobID, rasGrid.split('.')[-1])
    rasVRTRename = 'WSE_{}.{}'.format(jobID, rasVRT.split('.')[-1])

    for rawFilename in ras_grid_files:
        updateFilename = 'WSE_{}.{}'.format(jobID, rawFilename.split('.')[-1])
        os.rename(rawFilename, updateFilename)
    return rasGridRename

def get_proj_str(raster_src:any):
    """Handler for ogr/osr projections"""
    tiff_crs = osr.SpatialReference(raster_src.GetProjectionRef()).ExportToProj4()
    tiff_crs = rasterio.crs.CRS.from_proj4(tiff_crs).to_proj4()
    return rasterio.crs.CRS.from_string(tiff_crs)
    

def clean_workspace(wkdir:any, jobID:str, file_extenstions:list = ['.html', '.ipynb', '.csv', '.tif', '.vrt'])-> list:
    """Remove temporary files from post-processing dir"""
    tmp_files = list(wkdir.rglob('*'))
    save_files = [f for f in tmp_files if '{}{}'.format(jobID, f.suffix) in f.name and f.suffix in file_extenstions]

    for f in tmp_files:
        if os.path.isdir(str(f)):
            continue

        elif f.parent.name in ['Points', 'Terrains']:
            continue

        elif f in save_files:
            continue

        else:
            os.remove(f)

    #  Remove temporary, empty directories
    leftovers = list(wkdir.rglob('*'))
    for f in leftovers:
        if os.path.isdir(f):
            if len(list(pl.Path(f).rglob('*')))==0:
                os.rmdir(f)

    return save_files

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def s3_nbs(bucket:str, prefix:str, nameSelector:str='', fileformat:str='.ipynb') -> list:
    """
    Lists notebooks on S3 when provided with the bucket, object prefix, and
    file format. The default fileformat is IPython (i.e. Jupyter) Notebooks.
    """
    s3_client = boto3.client('s3')
    keys = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    keysList = [keys]
    pathsList = []
    
    while keys['IsTruncated'] is True:
        keys = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix,
                                         ContinuationToken=keys['NextContinuationToken'])
        keysList.append(keys)
    for key in keysList:
        key_matches = [elem['Key'] for elem in key['Contents'] if elem['Key'].find('{}'.format(nameSelector))>=0 and elem['Key'].endswith(fileformat)]
        paths = ['s3://{0}/{1}'.format(bucket, key) for  key in key_matches]
        pathsList.extend(paths)

    return pathsList