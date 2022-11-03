#James Gibson
#06/08/2022

#SDG Indicator 11.7.1
#Step 3: OPS


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
            #Get Data
            out_ucdb = '%s_ucdb' % iso
            out_le_polygon = '%s_osm_le_polygons' % iso
            out_la_polygon = '%s_osm_la_polygons' % iso
            out_na_polygon = '%s_osm_na_polygons' % iso
            out_roads = '%s_osm_roads' % iso
            df = pd.read_csv(RegionalRoadWidths)
            ## OPS ##
            #Merge landuse, leisure, and natural polygons into an OPS layer
            merged_ops = '%s_OPS_merged' % iso
            #arcpy.Delete_management(merged_ops)
            arcpy.Merge_management([out_le_polygon,out_la_polygon,out_na_polygon],merged_ops)
            #Dissolve
            arcpy.RepairGeometry_management(merged_ops)
            dissolved_ops = '%s_OPS_dissolved' % iso
            arcpy.PairwiseDissolve_analysis(merged_ops,dissolved_ops)
            #Clip to UCDB
            ucdb_ops_clip = '%s_ucdb_OPS_clip' % iso
            arcpy.RepairGeometry_management(dissolved_ops)
            arcpy.PairwiseClip_analysis(dissolved_ops,out_ucdb,ucdb_ops_clip)
            #Change Shape Area to Area km^2
            #arcpy.AddField_management(ucdb_ops_clip,"Area_squarekm_ops","DOUBLE")
            #arcpy.CalculateField_management(ucdb_ops_clip,"Area_squarekm_ops","!shape.geodesicArea@squarekilometers!","PYTHON")
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


