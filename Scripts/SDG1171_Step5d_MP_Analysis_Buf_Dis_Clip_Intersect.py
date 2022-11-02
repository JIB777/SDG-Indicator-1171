#06/08/2022

#SDG 11.7.1
#"Average share of the built-up area of cities that is open space for public use for all,
#by sex, age and persons with disabilities"
#Step 5d: Analysis

import arcpy, sys, os
from collections import defaultdict
from arcpy import env
from arcpy.sa import *
import time
import datetime
import pandas as pd
import numpy as np
import multiprocessing
arcpy.env.overwriteOutput = True


GADMGlobal = r'G:\HumanPlanet\GADM\GADM.gdb\GADMCopy3'
UCDB = r'G:\HumanPlanet\UCDB\Version2\GHS_STAT_UCDB2015MT_GLOBE_R2019A\JG_UCDB.gdb\GHS_STAT_UCDB2015MT_GLOBE_R2019A_V1_2'
RegionalRoadWidths = r'G:\HumanPlanet\G_Scripts\SDG1171\Version2\RegionalRoadWidths.csv'


#Start Time
Start_Time = time.time()

def process(iso):
    message = None
    if message is None:
        try:
            gdb = r'G:\HumanPlanet\SDG1171\Version3\Countries\%s.gdb' % iso
            arcpy.env.workspace = gdb
            #Get WP
            WP = r'G:\HumanPlanet\WorldPopData\unconstrained\2020\%s\%s_ppp_2020_UNadj.tif' % (iso,iso.lower())
            #Get Data
            ucdb_ops_clip = '%s_ucdb_OPS_clip' % iso
            out_ucdb = '%s_ucdb' % iso
            #Buffer
            out_buf = '%s_ucdb_access_buf' % iso
            #arcpy.Delete_management(out_buf)
            arcpy.PairwiseBuffer_analysis(ucdb_ops_clip,out_buf,'400 meters',"NONE","#","GEODESIC")
            #Dissolve
            out_dis = '%s_ucdb_access_dis' % iso
            #arcpy.Delete_management(out_dis)
            arcpy.PairwiseDissolve_analysis(out_buf,out_dis)
            #Clip
            out_clip = '%s_ucdb_access_clip' % iso
            #arcpy.Delete_management(out_clip)
            arcpy.PairwiseClip_analysis(out_dis,out_ucdb,out_clip)
            #Intersect
            ucdb_access_intersect = '%s_ucdb_access_intersect' % iso
            inFeatures = [out_ucdb,out_clip]
            #arcpy.Delete_management(ucdb_access_intersect)
            arcpy.analysis.PairwiseIntersect(inFeatures,ucdb_access_intersect,"ALL",None,"INPUT")
            message = 'Done: ' + iso
        except Exception as e:
            message = 'Failed: ' + iso + ' ' + str(e)

    return message

def main():
    print('Starting Script...')
    mylist=[]
    with arcpy.da.SearchCursor(UCDB,['CTR_MN_ISO']) as cursor:
        for row in cursor:
            if row[0] in mylist:
                pass
            else:
                mylist.append(row[0])
    length = len(mylist)
    print("Ready to start processing {} countries".format(length))
    pool = multiprocessing.Pool(processes=20, maxtasksperchild=1)
    results = pool.imap_unordered(process,mylist)
    counter = 0
    for result in results:
        print(result)
        counter = counter + 1
        print("{} countries processed out of {}".format(counter,length))
        print('---------------------------------------------------------')
    pool.close()
    pool.join()
    End_Time = time.time()
    Total_Time = End_Time - Start_Time
    print('Total Time: %s' % str(Total_Time))
    print('Script Complete')
    
    


if __name__ == '__main__':
    main()


