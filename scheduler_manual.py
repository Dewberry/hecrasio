import time
import subprocess
import numpy as np
import sys
sys.path.append('.')
sys.path.append(r'C:\Users\Administrator\Desktop')
import pathlib as pl
import os


procDir = sys.argv[1] # e.g. P1
table_name = sys.argv[2] # e.g. "simulations2"
jobKey  = sys.argv[3] # Runjob e.g. s3://pfra/DC/P06/H24/E2161/DC_P06_H24_E2161_in.zip
timeout = 30 # seconds to wait between looking for jobs

wkdir = pl.Path(r"C:\Users\Administrator\Desktop\PROCESSING\{}".format(procDir))
run_cmd = r'C:\Users\Administrator\Desktop\hecrasio\PostProcessor.py'



def main():

    job = jobKey.split('/')[-1].replace("_in.zip", "")

    print("update post process as active")
    print("Found job {} sending to {}".format(job, procDir))

    processing = subprocess.call(["python", run_cmd, job, procDir])

if __name__ == '__main__':
    main()