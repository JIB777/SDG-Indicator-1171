#James Gibson
#4/13/22

#SDG Indicator 11.7.1
#Step 2c: Extract OSM Natural polygons

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


#Start Time
Start_Time = time.time()

def process(iso):
    message = None
    if message is None:
        try:
            gdb = r'G:\HumanPlanet\SDG1171\Version3\Countries\%s.gdb' % iso
            arcpy.env.workspace = gdb
            #Global Variables
            #OPS Polygons
            natural_polygons = r'G:\HumanPlanet\OSM_data_2022\natural_EPSG4326 (1)\natural_2022.gdb\natural_EPSG4326_multipolygon'
            #UCDB Clip
            out_ucdb = '%s_ucdb' % iso
            #Extract natural polygons
            na_polygon = arcpy.SelectLayerByLocation_management(natural_polygons,'INTERSECT',out_ucdb)
            out_na_polygon = '%s_osm_na_polygons' % iso
            arcpy.CopyFeatures_management(na_polygon,out_na_polygon)
            #Filter natural tags
            keepList_natural = ['fell','grassland','heath','scrub','wood']
            with arcpy.da.UpdateCursor(out_na_polygon,['natural']) as cursor:
                for row in cursor:
                    if row[0] in keepList_natural:
                        pass
                    else:
                        cursor.deleteRow()
            #Filter natural access tags
            no_access = ['no','private']
            with arcpy.da.UpdateCursor(out_na_polygon,['access']) as cursor:
                for row in cursor:
                    if row[0] in no_access:
                        cursor.deleteRow()
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


