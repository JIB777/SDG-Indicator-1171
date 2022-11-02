#06/08/2022

#SDG Indicator 11.7.1
#"Average share of the built-up area of cities that is open space for public use for all,
#by sex, age and persons with disabilities"
#Step 5b: Analysis- Union

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
            ucdb_ops_clip = '%s_ucdb_OPS_clip' % iso
            ucdb_roads_clip = '%s_ucdb_roads_clip' % iso
            complete = '%s_complete' % iso
            ucdb_ops_roads_merge = '%s_ucdb_ops_roads_merge' % iso
            ucdb_merge_intersect = '%s_ucdb_merge_intersect' % iso
            #Make a copy
            complete = '%s_complete' % iso
            #arcpy.Delete_management(complete)
            arcpy.CopyFeatures_management(out_ucdb,complete)
            #Convert to km^2
            #Get total area of urban center
            #arcpy.DeleteField_management(complete,'TOTAL_AREA_SQKM')
            arcpy.AddField_management(complete,'TOTAL_AREA_SQKM','DOUBLE')
            arcpy.CalculateField_management(complete,'TOTAL_AREA_SQKM','!geom.geodesicArea@squarekilometers!','PYTHON')
            ##Get total area of public space
            #Dissolve
            #arcpy.DeleteField_management(ucdb_merge_intersect,'TOTAL_AREA_OPS_ROADS_SQKM')
            dissolve_fields = ['CTR_MN_NM','CTR_MN_ISO','UC_NM_MN','UC_NM_LST','UCDBID']
            out_dis = '%s_ucdb_merge_intersect_dis' % iso
            arcpy.PairwiseDissolve_analysis(ucdb_merge_intersect,out_dis,dissolve_fields)
            arcpy.AddField_management(out_dis,'TOTAL_AREA_OPS_ROADS_SQKM','DOUBLE')
            arcpy.CalculateField_management(out_dis,'TOTAL_AREA_OPS_ROADS_SQKM','!geom.geodesicArea@squarekilometers!','PYTHON')
            #join field
            arcpy.JoinField_management(complete,"UCDBID",out_dis,"UCDBID",["TOTAL_AREA_OPS_ROADS_SQKM"])
            #Divide total area of public space by total area of city
            #arcpy.DeleteField_management(complete,'Percent_Public_Space')
            arcpy.AddField_management(complete,'Percent_Public_Space','DOUBLE')
            expression = "(!TOTAL_AREA_OPS_ROADS_SQKM!/!TOTAL_AREA_SQKM!)*100"
            arcpy.CalculateField_management(complete,'Percent_Public_Space',expression,'PYTHON')
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


