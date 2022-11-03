#James Gibson
#4/13/22

#SDG Indicator 11.7.1
#Step 2d: Extract OSM roads

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
            #Roads
            roads = r'G:\HumanPlanet\SDG91\Version2\OSM Highway Data\highway_EPSG4326\JG_OSM_Roads_Extract.gdb\highway_EPSG4326_line'
            #UCDB Clip
            out_ucdb = '%s_ucdb' % iso
            #Extract roads
            out_roads = '%s_osm_roads' % iso
            result = arcpy.SelectLayerByLocation_management(roads,"INTERSECT",out_ucdb)
            arcpy.CopyFeatures_management(result,out_roads)
            #Filter highway tag
            keepList_Roads = ['motorway','trunk','primary','secondary','tertiary',
                              'unclassified','residential','motorway_link','trunk_link','primary_link',
                              'secondary_link','tertiary_link','living_street','pedestrian','road',
                              'busway','sidewalk','cycleway','footway','track',
                              'bridleway','path']
            with arcpy.da.UpdateCursor(out_roads,['highway']) as cursor:
                for row in cursor:
                    if row[0] in keepList_Roads:
                        pass
                    else:
                        cursor.deleteRow()
            #Filter highway access tags
            no_access = ['no','private']
            with arcpy.da.UpdateCursor(out_roads,['access']) as cursor:
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


