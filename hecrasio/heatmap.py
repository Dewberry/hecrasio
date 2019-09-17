"""
PFRA Module for creating heatmap tifs.
"""

import os
import psutil
import boto3
import rasterio
import gdal
import h5py
from shapely.geometry import Polygon
from glob import glob
from multiprocessing import cpu_count
from io import BytesIO
import numpy as np
from rasterio.mask import mask

gdal.UseExceptions()
s3 = boto3.resource("s3")


def s3List(bucketName: str, prefixName: str, nameSelector: str, fileformat: str) -> list:
    """Returns an unlimited list of files on S3 when provided with an
        S3 bucket and object prefix. Files can be filered by to those
        meeting specific naming conventions with the name selector
        and/or file formats.
    """
    # Get a list of objects (keys) within a specific bucket and prefix on S3
    s3 = boto3.client("s3")
    keys = s3.list_objects_v2(Bucket=bucketName, Prefix=prefixName)
    # Store keys in a list
    keysList = [keys]
    # While the boto3 returned objects contains a value of true for 'IsTruncated'
    while keys["IsTruncated"] is True:
        # Append to the list of keys
        keys = s3.list_objects_v2(Bucket=bucketName, Prefix=prefixName, ContinuationToken=keys["NextContinuationToken"])
        keysList.append(keys)
    # Create a list of objects from the supplied keys
    pathsList = []
    for key in keysList:
        paths = ["s3://" + bucketName + "/" + elem["Key"]
                 for elem in key["Contents"]
                 if elem["Key"].find("{}".format(nameSelector)) >= 0
                 and elem["Key"].endswith(fileformat)]
        pathsList = pathsList + paths
    return pathsList


def getTifData_S3(s3path):
    """Read a raster from S3 into memory and get attributes"""
    s3 = boto3.resource("s3")
    if isinstance(s3path, str):
        bucket_name = s3path.split(r"s3://")[1].split(r"/")[0]
        key = s3path.split(r"{}/".format(bucket_name))[1]
        s3tif = s3.Object(bucket_name=bucket_name, key=key)
        image_data = BytesIO(s3tif.get()["Body"].read())
    else:
        image_data = BytesIO(s3path.get()["Body"].read())
    tif_inmem = "/vsimem/data.tif"  # Virtual Folder to Store Data
    gdal.FileFromMemBuffer(tif_inmem, image_data.read())
    src = gdal.Open(tif_inmem)
    rb, gt = src.GetRasterBand(1), src.GetGeoTransform()
    null_value = rb.GetNoDataValue()
    return rb, gt, src, null_value


def bool_wse_to_hdf(wse_grid: str, model_run_id: str, h5: str, n_row_slices: int):
    """
    Reads in raster data as chunks (blocks), row-wise, and outputs
    to a Hierarchical Data Format (HDF).
    """
    rb, gt, src, null_value = getTifData_S3(wse_grid)
    xsize = rb.XSize
    ysize = rb.YSize
    ystep = int(ysize / n_row_slices)  # rows
    yresidual = ysize - (ystep * n_row_slices)  # last row
    for i in range(n_row_slices):
        if i != n_row_slices - 1:
            chunk = rb.ReadAsArray(0, ystep * i, xsize, ystep)
            chunk_bool = chunk != null_value
            with h5py.File(h5, "a") as hf:
                hf.create_dataset("chunk{}".format(i),
                                  data=chunk_bool.astype(np.int8),
                                  compression="gzip",
                                  compression_opts=9)
        else:
            chunk = rb.ReadAsArray(0, ystep * i, xsize, ystep + yresidual)
            chunk_bool = chunk != null_value
            with h5py.File(h5, "a") as hf:
                hf.create_dataset(
                    "chunk{}".format(i),
                    data=chunk_bool.astype(np.int8),
                    compression="gzip",
                    compression_opts=9,
                )
    ds = None
    return None


def daskbag_bool_wse_hdf_local(wse_grid: str, num_chunks: int, bool_dir: str = "bool_hdfs"):
    """Dask wrapper for bool_wse_to_hdf function"""
    try:
        if not os.path.exists(bool_dir):
            os.mkdir(bool_dir)
    except FileExistsError:
        pass
    model_run_id = os.path.basename(wse_grid).split(".")[0]
    h5 = os.path.join(bool_dir, f"bool_{model_run_id}.hdf")
    bool_wse_to_hdf(wse_grid, model_run_id, h5, n_row_slices=num_chunks)
    return None


def write_weighted_chunks_local(c: int,
                                weights_dict: dict,
                                bool_dir: str = "bool_hdfs",
                                weighted_dir: str = "weighted_chunks"):
    """Apply weights to each chunk across many bool hdfs, then write 1 hdf per chunk"""
    try:
        if not os.path.exists(weighted_dir):
            os.mkdir(weighted_dir)
    except FileExistsError:
        pass
    filelist = glob(os.path.join(bool_dir, "*.hdf"))
    for i, f in enumerate(filelist):
        run_id = f.split("_")[-2] + '_' + f.split("_")[-1].split(".")[0]
        weight = weights_dict[run_id]
        if i == 0:
            with h5py.File(f, "r") as hf:
                chunk = f"chunk{c}"
                data = np.array(hf[chunk]) * weight
        else:
            with h5py.File(f, "r") as hf:
                chunk = f"chunk{c}"
                data += np.array(hf[chunk]) * weight
    weighted_outfile = os.path.join(weighted_dir, f"weighted_{c}_.hdf")
    with h5py.File(weighted_outfile, "a") as hfout:
        hfout.create_dataset("chunk", data=data, compression="gzip", compression_opts=9)
    return


def update_tif_metadata(outputTif, meta_dict):
    """Update the metadata of a tif using a dictionary"""
    ds = gdal.Open(outputTif, gdal.GA_Update)
    ds.BuildOverviews()
    for k, v in meta_dict.items():
        ds.SetMetadataItem(k, v)
    ds = None
    return


def get_s3template_tif(grid_list: list):
    """Download the first WSE tif from s3 to use as a template"""
    os.system(f"aws s3 cp {grid_list[0]} .")
    tifTemplate = os.path.basename(grid_list[0])
    return tifTemplate


def get_num_chunks_local(tif):
    """
    Evaluates the size of the raster in memory and returns 
    the number of chunks and workers that should be used.
    """
    rb, gt, src, null_value = getTifData_S3(tif)
    xsize = rb.XSize
    ysize = rb.YSize
    in_mem = xsize * ysize * 4 / 1e9
    print(f"Opening this raster will equate to roughly {round(in_mem,2)} GB in memory.")
    exact_num_chunks = 1
    while (in_mem * 3) / exact_num_chunks > 2:
        exact_num_chunks += 1
    if exact_num_chunks < 10:
        num_chunks = 10
    elif exact_num_chunks < 20:
        num_chunks = 20
    else:
        print("This is a huge raster. `num_chunks` is being set to 30.")
        num_chunks = 30
    print(f"Using {num_chunks} for the number of chunks.")
    my_mem = psutil.virtual_memory().total / 1e9
    chunk_mem = in_mem * 3 / num_chunks
    num_workers = int(my_mem / chunk_mem)
    if num_workers > 2.5 * cpu_count():
        num_workers = int(2.5 * cpu_count())
    print(f"Using {num_workers} for the number of workers.")
    return num_chunks, num_workers


def writeTifByChunks_local(tifTemplate: str, outfile: str, chunk_hdfs: list, heatmap_dir: str):
    """
    Given a sorted list of local HDF files representing chunks of a tif,
    write the final output tif in chunks (for memory management).
    """
    if not os.path.exists(heatmap_dir):
        os.mkdir(heatmap_dir)
    src = rasterio.open(tifTemplate)
    with rasterio.Env():
        profile = src.profile
        profile.update(dtype=rasterio.float32, count=1, nodata=0, compress="lzw")
        with rasterio.open(os.path.join(heatmap_dir, outfile), "w", **profile) as dst:
            for i, f in enumerate(chunk_hdfs):
                with h5py.File(f, "r") as hf:
                    chunkArray = np.array(hf["chunk"])
                if i == 0:
                    ystart = 0
                    ystop = chunkArray.shape[0]
                    dst.write(chunkArray.astype(rasterio.float32),
                              1,
                              window=((ystart, ystop), (0, chunkArray.shape[1])))
                else:
                    ystart += chunkArray.shape[0]
                    ystop += chunkArray.shape[0]
                    dst.write(chunkArray.astype(rasterio.float32),
                              1,
                              window=((ystart, ystop), (0, chunkArray.shape[1])))
                del chunkArray
    return print(f"{os.path.join(heatmap_dir, outfile)} has been written!")


def enough_mem_to_clip(filepath: str):
    """Check if there is enough virtual memory on your machine to clip the raster"""
    ds = gdal.Open(filepath)
    rb = ds.GetRasterBand(1)
    xsize = rb.XSize
    ysize = rb.YSize
    ds = None
    mem_needed = xsize * ysize * 16 / 1e9
    mem_avail = psutil.virtual_memory().total / 1e9
    if mem_needed > mem_avail:
        return False
    else:
        return True


def clip_rast(polygon, raster, out_path):
    """
    Clip a raster using a polygon (geojson format or shapely). 
    Arguments raster and out_path are local path strings.
    """
    with rasterio.open(raster) as data:
        out_image, out_transform = rasterio.mask.mask(data, polygon, nodata=data.nodata, crop=True)
        out_meta = data.meta.copy()
    out_meta.update(
        {
            "driver": "GTiff",
            "nodata": data.nodata,
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform,
            "crs": data.crs,
            "compress": "lzw",
            "tiled": True,
        }
    )
    with rasterio.open(out_path, "w", **out_meta) as dest:
        dest.write(out_image)
    return


def clip_off_nodatas(in_filename, out_filename):
    """
    Use a mask to clip off no cells in the raster.
    """
    raster = rasterio.open(in_filename)
    msk = raster.read_masks(1)
    nz = np.nonzero(msk)
    ymax, xmax = max(nz[0]), max(nz[1])
    ymin, xmin = min(nz[0]), min(nz[1])
    ul = raster.transform * (xmin, ymax)
    ur = raster.transform * (xmax, ymax)
    lr = raster.transform * (xmax, ymin)
    ll = raster.transform * (xmin, ymin)
    clip_poly = Polygon([ul, ur, lr, ll])
    raster.close()
    clip_rast([clip_poly], in_filename, out_filename)
    return
