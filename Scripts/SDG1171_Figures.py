#08/30/22

#SDG 11.7.1
#Create bar graph of regional road widths database

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


RegionalRoadWidths = r'G:\HumanPlanet\G_Scripts\SDG1171\Version2\RegionalRoadWidths.csv'
df = pd.read_csv(RegionalRoadWidths)

df_subset = df[['Region','Median_Road_Width_m']].drop_duplicates()

regions = df_subset['Region'].to_list()
widths = df_subset['Median_Road_Width_m'].to_list()

length = len(regions)
locs = list(range(0,length))

plt.bar(regions,widths)
plt.title('OpenStreetMap (OSM) Derived Regional Median Road Widths',font='Times New Roman')
plt.xticks(locs,regions,rotation=90,font='Times New Roman')
plt.yticks(font='Times New Roman')
plt.xlabel('Region',font='Times New Roman')
plt.ylabel('Width (m)',font='Times New Roman')
plt.tight_layout()
plt.show()
