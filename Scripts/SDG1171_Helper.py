#10/05/22

#SDG 11.7.1
#Helper script

import arcpy, sys, os
from collections import defaultdict
from arcpy import env
from arcpy.sa import *
import time
import datetime
import pandas as pd
import numpy as np
import multiprocessing
import matplotlib.pyplot as plt
arcpy.env.overwriteOutput = True


leisure_polygons_gpkg = r'G:\HumanPlanet\OSM_data_2022\leisure_EPSG4326 (1)\leisure_EPSG4326.gpkg\main.leisure_EPSG4326_multipolygon'
landuse_polygons_gpkg = r'G:\HumanPlanet\OSM_data_2022\landuse_EPSG4326 (1)\landuse_EPSG4326.gpkg\main.landuse_EPSG4326_multipolygon'
natural_polygons_gpkg = r'G:\HumanPlanet\OSM_data_2022\natural_EPSG4326 (1)\natural_EPSG4326.gpkg\main.natural_EPSG4326_multipolygon'

leisure_polygons = r'G:\HumanPlanet\OSM_data_2022\leisure_EPSG4326 (1)\leisure_2022.gdb\leisure_EPSG4326_multipolygon'
landuse_polygons = r'G:\HumanPlanet\OSM_data_2022\landuse_EPSG4326 (1)\landuse_2022.gdb\landuse_EPSG4326_multipolygon'
natural_polygons = r'G:\HumanPlanet\OSM_data_2022\natural_EPSG4326 (1)\natural_2022.gdb\natural_EPSG4326_multipolygon'

#Leisure
num_leisure_gpkg = arcpy.GetCount_management(leisure_polygons_gpkg)
num_leisure = arcpy.GetCount_management(leisure_polygons)
print('Number of Leisure Polygons in Geopackage: %s' % str(num_leisure_gpkg))
print('Number of Leisure Polygons in Extract: %s' % str(num_leisure))
print('----------------')

#Landuse
num_landuse_gpkg = arcpy.GetCount_management(landuse_polygons_gpkg)
num_landuse = arcpy.GetCount_management(landuse_polygons)
print('Number of Landuse Polygons in Geopackage: %s' % str(num_landuse_gpkg))
print('Number of Landuse Polygons in Extract: %s' % str(num_landuse))
print('----------------')

#Natural
num_natural_gpkg = arcpy.GetCount_management(natural_polygons_gpkg)
num_natural = arcpy.GetCount_management(natural_polygons)
print('Number of Natural Polygons in Geopackage: %s' % str(num_natural_gpkg))
print('Number of Natural Polygons in Extract: %s' % str(num_natural))
