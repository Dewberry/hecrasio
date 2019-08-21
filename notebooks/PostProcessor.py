#!/usr/bin/env python
# coding: utf-8

# ### Dev routine for PFRA Post-Processing
import sys
sys.path.append('../')
from hecrasio.core import *
from hecrasio.qaqc import *
from hecrasio.s3tools import *



def main():
    jobid = sys.argv[1] # JobID to process
    tmpID = sys.argv[2] # Integer for naming processing folder if required
    
    # ### Pull from queue
    nb = r'C:\Users\Administrator\Desktop\hecrasio\notebooks\{}'.format('QAQC-PFRA.ipynb')
    #cmd = r"C:\Users\Administrator\Desktop\bins\Mapper64\RasComputeMaps.exe"
    cmd = r'C:\Program Files (x86)\HEC\HEC-RAS\5.0.7\Mapper64\RasComputeMaps.exe'

    # Assign Wkdir ID for running multiple
    TMPID = 1
    wkdir = pl.Path(r"C:\Users\Administrator\Desktop\P{}".format(TMPID))
    ### Create Data Directory if needed
    if not os.path.exists(wkdir): os.mkdir(wkdir)


    # Paths created on the fly using dirs
    proj_dir = pl.Path(r"C:\Users\Administrator\Desktop\{}".format(projID))
    terrain_dir = proj_dir/"Terrain"
    points_dir = proj_dir/"Points" # \{0}_Points.shp"
    s3_model_input, s3_model_output, s3_point_data, s3_output_dir = get_model_paths(jobID)

    proj_paths = [proj_dir, terrain_dir, points_dir]
    for p in proj_paths:
        if not os.path.exists(p):
            os.mkdir(p)
            
    # Get Point & Terrain Data if needed
    local_point_data = points_dir/'{}_Points.shp'.format(projID)

    if not os.path.exists(local_point_data):
        os.chdir(points_dir)
        get_point_from_s3(s3_point_data)
        os.chdir('../')
        
    if len(os.listdir(terrain_dir)) < 4:
        os.chdir(terrain_dir)
        get_terrain_data(terrain_dir, s3_model_input)
        os.chdir('../')
        
    # Create RASMAP Inputs
    os.chdir(wkdir)
    rasmap  = str(wkdir/"{}.rasmap")
    rasPlan = str(wkdir/"{}")
    qaqcNB  = str(wkdir/"{}.ipynb".format(jobID))


    # Run QAQC Notebook
    notebook = pm.execute_notebook(nb, qaqcNB, parameters={'model_s3path' : s3_model_output})
    pipe = subprocess.Popen(['jupyter', 'nbconvert', qaqcNB], stdout=subprocess.PIPE)


    # Get List of tiffs used in model 
    terrainHDF = list(terrain_dir.glob('*.hdf'))[0]
    terrainTIF = list(terrain_dir.glob('*.tif'))[0]
    terrainVRT = list(terrain_dir.glob('*.vrt'))[0]
    projection_file_name = list(terrain_dir.glob('*.prj'))[0]

    # Change to wkdir to process results
    os.chdir(wkdir)
    planFile = [p for p in os.listdir() if jobID in p and '.hdf' in p][0]

    # Generate RASMAP file
    rasmap_xml = write_rasmap_file(projection_file_name, jobID, str(terrainTIF))
    with open('{}.rasmap'.format(jobID), 'w') as f: f.write(rasmap_xml)

    # Call RasMapper to generate tif
    pipe = subprocess.Popen([cmd, rasmap.format(jobID), rasPlan.format(planFile)], stdout=subprocess.PIPE)
    pipe_text = pipe.communicate()[0].decode("utf-8")
    check_map_created(pipe_text)
    assert check_map_created(pipe_text), 'Error Creating Map'


    # Move wsel tiffs from plan directory to wkdir
    collect_output_data(jobID) 

    # Read in point & wsel data
    points = PointData(local_point_data)
    local_tiff = GridObject(rasGridRename)
    points = points.geodataframe.to_crs(get_proj_str(local_tiff.src))

    # Attribute points from wsel
    act_pointdata_results = query_gdf(points, local_tiff.gt, local_tiff.rb, 'plus_code')
    df = pd.DataFrame.from_dict(act_pointdata_results, orient = 'index', columns=[jobID])
    df.to_csv('{}.csv'.format(jobID))

    # Clean tmp files & copy results to s3
    save_files = clean_workspace(wkdir)
    assert len(save_files) == 5


    for s in save_files:
        s3file = s3_output_dir.replace('s3://pfra/','') +'/'+ s.name
        upload_file(str(s),'pfra', s3file)

if __name__== "__main__":
    main()