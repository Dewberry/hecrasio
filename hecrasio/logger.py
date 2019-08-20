import os
import time

def log(in_dir, file_name, in_text= None, start= None, end= None, exception= None):
    """Required modules: os, time
    
    This function notates a text file based on arguments passed. 
    This function is intended for documentation and debugging.
    
    Positional arguments-
        in_dir: Document path for text file (String)
        file_name: Text file to create or appended to (String)
    
    Optional arguments-
        in_text: Line to write into text file (String)
        start: Tells func to write opening line (boolean)
        end: Tells func to write closing line (boolean)
        exception: Pass an exception to log the message (Exception)
    
    General usage:
    Set static vaiables for [in_directory](s) and [file_name](s)
    and feed in string values thru [in_text] that will give 
    you insightful feedback on your code (i.e. 
    arcpy.GetMessages() string as a variable).
    This function can also be used to create README files by passing 
    in block text. This func always logs at the end of the specified 
    file, and can be used to keep records of multiple runs.
    
    Try Me:
    
    log(in_dir, file_name, start=True)
    # Operation 1
    for i in range(24):
        msg = "Op1 - {} of 24 complete".format(str(i+1))
        log(in_dir, file_name, in_text= msg)
    # Operation 2
    try:
        1/0 # Causes intentional exception
        log(in_dir, file_name, in_text= "Op2 - Succeeded")
    except Exception as e:
        log(in_dir, file_name, in_text= "Op2 - Failed", exception= e)
    log(in_dir, file_name, end=True) 
    """
    if in_text != None:
        os.chdir(in_dir)
        readme = open(file_name, "a")
        readme.write(in_text + "\n")
        readme.close()
        
    if exception != None:
        os.chdir(in_dir)
        readme = open(file_name, "a")
        if hasattr(exception, 'message'):
            readme.write("\nException message:" + "\n")
            readme.write(str(exception.message) + "\n")
        else:
            readme.write("\nException message:" + "\n")
            readme.write(str(exception) + "\n")
        readme.close()
    
    if start == True:
        os.chdir(in_dir)
        readme = open(file_name, "a")
        readme.write("\n#-------------------------------------#\n\n")
        readme.write("Script start: " + time.ctime() + "\n\n")
        readme.close()

    if end == True:
        os.chdir(in_dir)
        readme = open(file_name, "a")
        end_time = time.time()
        readme.write("\nScript end: " + time.ctime() + "\n")
        readme.close()  