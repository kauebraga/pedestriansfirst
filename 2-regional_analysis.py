import warnings
warnings.simplefilter(action = "ignore", category = FutureWarning)

from funs.pedestriansfirst import *
from funs.get_jurisdictions import get_jurisdictions
from funs.get_number_jurisdictions import *
from funs.prep_poly import *
# from funs.process_bike_new import process_bike
import subprocess
import os
import os.path
import json
import shutil
import geojson
import shapely
import datetime
from shapely.geometry import LineString, Polygon, shape, mapping
from shapely.ops import unary_union
import geopandas as gpd
import topojson
import pandas as pd
import numpy as np
import math
import osmnx as ox
import warnings
import urllib
import zipfile
from tqdm import tqdm
import sys
import pdb
import timeit
import csv

import subprocess
import os
import os.path
import json
import shutil
import geojson
import shapely
import datetime
from shapely.geometry import LineString, Polygon, shape, mapping
from shapely.ops import unary_union
import geopandas as gpd
import topojson
import pandas as pd
import numpy as np
import math
import osmnx as ox
import warnings
import urllib
import zipfile
from tqdm import tqdm
import sys

import pdb



import pdb

import funs.pedestriansfirst
import re
import funs.calculate_country_ind
from funs.calculate_country_ind import *

    
useful_tags = ox.settings.useful_tags_way + ['cycleway', 'cycleway:left', 'cycleway:right', 'cycleway:both', 'bicycle']
ox.settings.log_console = True
ox.settings.use_cache = True
ox.settings.useful_tags_way = useful_tags
ox.settings.overpass_rate_limit = False

# hdc = 11480
# hdc = "02007" # los angeles
# hdc = "08154" # fortaleza
# hdc = "07277" # sao paulo
# hdc = 40
# hdc = 8099 # new york
# hdc = "01156" # trujillo
# hdc = "05472" # jakarta
# hdc = "05816" # london
# hdc = "00992" # amman
# hdc = "99999" # kohima


# folder_prefix = 'cities_out'; current_year=2023; minimum_portion=0.6;


def regional_analysis(hdc, to_test, folder_prefix = 'cities_out', minimum_portion=0.6, prep = True, analyse=True,summarize=True,jurisdictions=True,simplification=0.001, current_year=2024, cleanup = False):
    """Run analysis scripts for a given region
    
    Uses lots (too many?) hardcoded defaults
    
    1) sets up directory structure for an agglomeration
    
    2) calls get_jurisdictions to identify analysis areas within agglomeration
    
    3) prepares OSM data
    
    4)'Analysis' creates all the geodata needed to calculate
    Atlas indicators for a given analysis area (eg., isochrone polys). 
    
    5)'Summary' actually calculates those indicators for each analysis area
    within the agglomeration
    
    
    """
    
    df = pd.DataFrame()
    
    # Define the CSV file path
    csv_file = f"logs/running_{hdc}.csv"
    df.to_csv(csv_file, mode='a', header=True, index=False)
    
    #1) set up directories
    if not os.path.isdir('temp/'):
        os.mkdir('temp/')
    if not os.path.isdir('cities_out/'):
        os.mkdir('cities_out/')
    if folder_prefix:
        folder_name = folder_prefix+'/ghsl_region_'+str(hdc)+'/'
    else:
        folder_name = str(hdc)+'/'
    if not os.path.isdir(str(folder_name)):
        os.mkdir(str(folder_name))
    if not os.path.isdir(str(folder_name)+'/debug/'):
        os.mkdir(str(folder_name)+'/debug/')
    if not os.path.isdir(str(folder_name)+'/temp/'):
        os.mkdir(str(folder_name)+'/temp/')
    
    if os.path.isfile(f"{folder_name}geodata/blocks/blocks_latlon_{current_year}.geojson"):
        analyze = False
        
    #2) get analysis areas
    
    
    
    if jurisdictions == True:
      analysis_areas = get_jurisdictions(
          hdc, 
          minimum_portion=minimum_portion
          )
      
      analysis_areas.to_file(f'{folder_name}/debug/analysis_areas.gpkg', driver='GPKG')
    else:
      analysis_areas = gpd.read_file(f'{folder_name}/debug/analysis_areas.gpkg')    
      

    
    
    #If we're going to do any access-based indicators
    #Let's make sure to buffer this to include peripheral roads etc for routing
    total_poly_latlon=analysis_areas.unary_union
    total_poly_latlon = shapely.convex_hull(total_poly_latlon)   
    # total_poly_latlon.to_file(f'{folder_name}/debug/boundary.gpkg', driver='GPKG')
    gpd.GeoDataFrame(geometry=[total_poly_latlon], crs=4326).to_file(f'{folder_name}/debug/area_for_osm_extract.gpkg', driver='GPKG')
    
    #3) prepare OSM data files for the agglomeration
    if prep == True:
      start = timeit.default_timer()
      prep_from_poly(hdc, total_poly_latlon, folder_name, boundary_buffer = 2000, current_year=current_year)
      stop = timeit.default_timer()
      print('Time prep: ', stop - start)  
    else:
      print('Didnt prep')  
    
    #4) now actually call the functions and save the results
    if analyse:
      print("why?")
      start = timeit.default_timer()
      geospatial_calctime = spatial_analysis(
          boundaries = total_poly_latlon,
          id_code = hdc,
          name = analysis_areas.loc[0,'name'],
          folder_name=folder_name,
          current_year = current_year,
          to_test = to_test
          # [    
                         # 'healthcare',
                         # 'schools',
                         # 'hs',
                         # 'bikeshare',
                         # 'carfree',
                         # 'blocks',
                         # 'density',
                         # 'pnft', # VOLTAR PRO PADRAO!!!
                         # 'pnrt',
                         # 'pnpb', #protected bikeways
                         # 'pnab', #all bikeways
                         # 'pnst', #combo transit + bike
                         # 'highways',
                         # ]
          )
      stop = timeit.default_timer()
      print('Time analyze: ', stop - start)  
    else:
      print('Didnt analyze')  
      geospatial_calctime = 0
    
    #5) calculate indicator measurement for each analysis area
    if summarize == True:
        start = datetime.datetime.now()
        calculate_indicators(
            analysis_areas = analysis_areas, 
            folder_name=folder_name,
            )
        end = datetime.datetime.now()
        analysis_areas.loc[0, 'geospatial_calctime'] = str(geospatial_calctime)
        analysis_areas.loc[0, 'summary_calctime'] = str(end - start)
        
        topo = topojson.Topology(analysis_areas, prequantize=True)
        analysis_areas = topo.toposimplify(0.001).to_gdf()
        analysis_areas.to_file(f'{folder_name}indicator_values.gpkg',driver='GPKG')
        
        nongeospatial_results = analysis_areas.drop('geometry', axis=1, inplace=False)
        nongeospatial_results.to_csv(f'{folder_name}indicator_values.csv')
        
    # Define the CSV file path
    os.remove(csv_file)
    csv_file = f"logs/finished_{hdc}.csv"
    df.to_csv(csv_file, mode='a', header=True, index=False)
    
    #clean up big files
    if cleanup:
        for cleanup_filename in ['city.o5m', 'city.pbf','cityhighways.o5m','citywalk.o5m','access/city_ltstagged.pbf']:
            if os.path.exists(f'{folder_name}/temp/{cleanup_filename}'):
                os.remove(f'{folder_name}/temp/{cleanup_filename}')
        for file in os.listdir('cache/'):
            os.remove(f'cache/{file}')
    #import pdb; pdb.set_trace()


import shutil


# ny
hdc = "08099"
shutil.rmtree(f'cities_out/ghsl_region_{hdc}')
start = timeit.default_timer()
regional_analysis(hdc = hdc, analyze=True, summarize=True)
stop = timeit.default_timer()
print('Time prep: ', stop - start)
# Time prep:  3433.462432500004

# los angeles
hdc = "02007"
shutil.rmtree(f'cities_out/ghsl_region_{hdc}')
start = timeit.default_timer()
regional_analysis(hdc = hdc, analyze=True, summarize=True)
stop = timeit.default_timer()
print('Time prep: ', stop - start)
# Time prep:  2551.689492332982


# fortaleza
hdc = "08154"
shutil.rmtree(f'cities_out/ghsl_region_{hdc}')
start = timeit.default_timer()
regional_analysis(hdc = hdc, analyze=True, summarize=True)
# old fortaleza
# regional_analysis(hdc = 1397, analyze=True, summarize=True)
stop = timeit.default_timer()
print('Time final: ', stop - start)


# sp
hdc = "00507"
shutil.rmtree(f'cities_out/ghsl_region_{hdc}')
start = timeit.default_timer()
regional_analysis(hdc = hdc, analyze=True, summarize=True)
# old fortaleza
# regional_analysis(hdc = 1397, analyze=True, summarize=True)
stop = timeit.default_timer()
print('Time final: ', stop - start)



calculate_country_indicators(current_year = 2023)




# for 2024 - we are only running partially
hdcs = pd.read_csv('input_data/hdc_to_run.csv', dtype={'hdc_new' : str})

# 1) Run only the population data collection for the cities we already have data from the previous collection
hdcs_notrun = hdcs[~hdcs['run_again']]
hdcs_notrun = hdcs_notrun["hdc_new"].tolist()

# regional_analysis(hdc = "08154", to_test = [], analyze=True, summarize=False, current_year=2024, prep = False, jurisdictions=False)


for hdc in hdcs_notrun[850:1019]:
  regional_analysis(hdc, to_test = [], analyze=True, summarize=False, jurisdictions=True, current_year=2024)


# we need to recalculate PNRT for all --------
hdcs_all = hdcs["hdc_new"].tolist()

start = timeit.default_timer()
for hdc in hdcs_all[1:5]:
  regional_analysis(hdc, to_test = ['pnrt'], analyse=True, summarize=False, 
    jurisdictions=False, current_year=2024, prep=False)
end = timeit.default_timer()
  
  
from concurrent.futures import ThreadPoolExecutor

def run_analysis(hdc):
    return regional_analysis(
        hdc,
        to_test=['pnrt'],
        analyse=True,
        summarize=False,
        jurisdictions=True,
        current_year=2024,
        prep=False,
    )

if __name__ == "__main__":
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(run_analysis, hdcs_all))
  

# AFTER HERE, WE COPY ALL THE OTHER FILES FROM THE PREVIOUS RUN


# 2) Run all the data collection for the city we don't have data
hdcs_run = hdcs[hdcs['run_again']]
hdcs_run = hdcs_run["hdc_new"].tolist()

for hdc in hdcs_run[65:95]:
  regional_analysis(hdc, 
                    to_test =  [    
                           'healthcare',
                           'schools',
                           'hs',
                           'bikeshare',
                           'carfree',
                           'blocks',
                           'density',
                           'pnft', # VOLTAR PRO PADRAO!!!
                           'pnrt',
                           'pnpb', #protected bikeways
                           'pnab', #all bikeways
                           'pnst', #combo transit + bike
                           'highways',
                           ],
                    analyze=True, 
                    summarize=False, 
                    current_year=2024)
                    
# regional_analysis(hdc = "01156", analyze=True, to_test = ['pnft'], summarize = True, current_year=2024, prep = False, jurisdictions=False)
# regional_analysis(hdc = "05472", analyze=False, to_test = ['pnft'], summarize = True, current_year=2024, prep = False, jurisdictions=False)
regional_analysis(hdc = "00507", analyse=False, to_test = [], summarize = True, current_year=2024, prep = False, jurisdictions=True)

# run other cities (from ITDP ) too
# or no?


# 3) Finally, calculate the indicators for every city
hdcs_all = hdcs["hdc_new"].tolist()

for hdc in hdcs_all:
  regional_analysis(hdc, to_test = [], jurisdictions = False, prep = False, 
                    analyze=False, summarize=True,  current_year=2024)


# 4) Then calculate the country indicators
calculate_country_indicators()





# all ----------------
ucdb = gpd.read_file('input_data/ghsl/ghsl_2024.gpkg')
# ucdb = gpd.read_file('input_data/ghsl/SMOD_V1s6_opr_P2023_v1_2020_labelUC_DB_release.gpkg')
ucdb.index =  ucdb['ID_UC_G0']
hdcs_to_test += list(ucdb[(int(sys.argv[2]) < ucdb['POP_2020'])&(ucdb['POP_2020'] < int(sys.argv[1]))].sort_values('POP_2020', ascending=False).ID_UC_G0)
hdcs_to_test = [11480 , 461 , 576 , 8265 , 4494] #ITDP "Cycling Cities" below 500k
for hdc in hdcs_to_test:
    hdc = int(hdc)
    #if len(sys.argv) == 1:
    #divide_by = 1
    #remainder = 0
    # else: 
    divide_by = int(sys.argv[3])
    remainder = int(sys.argv[4])
    print (f"{hdc}%{divide_by}={hdc % divide_by}, compare to {remainder}, {ucdb.loc[hdc,'NAME_MAIN']}, pop {ucdb.loc[hdc,'POP_2020']}")
    if hdc % divide_by == remainder and ucdb.loc[hdc,'NAME_MAIN'] != 'N/A':
        if not os.path.exists(f'cities_out/ghsl_region_{hdc}/indicator_values.csv'):
            if os.path.exists(f'cities_out/ghsl_region_{hdc}/geodata/blocks/blocks_latlon_2024.geojson'):
                regional_analysis(hdc)#, analyze=False)
            else:
                regional_analysis(hdc)
                
                
                
                
                
# in parallel??

from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import ProcessPoolExecutor

ucdb = pd.read_csv('input_data/pbf/pbf_hdc_country.csv', dtype={'hdc' : str})
ucdb_ok = ucdb[ucdb['pop'] >= 500000]
ucdb_ok_africa = ucdb_ok[ucdb_ok['region'] == "africa"]
hdcs_to_test = ucdb_ok_africa['hdc'].tolist()
# hdcs_to_test = ''.join(hdcs_to_test)


# Function to process each HDC
def process_hdc(hdc):
    try:
        hdc = int(hdc)
        print(f"Starting analysis for HDC: {hdc}")
        regional_analysis(hdc)  # Call your function here
        print(f"Finished analysis for HDC: {hdc}")
    except Exception as e:
        print(f"Error processing HDC {hdc}: {e}")
    
start = timeit.default_timer()
if __name__ == "__main__":
    with ThreadPoolExecutor() as executor:
        # Submit all tasks and collect futures
        futures = [executor.submit(process_hdc, hdc) for hdc in hdcs_to_test[0:50]]

        # Optionally track progress or handle results
        for future in as_completed(futures):
            try:
                future.result()  # This will re-raise any exception from the thread
            except Exception as e:
                print(f"Thread raised an exception: {e}")
stop = timeit.default_timer()
print('Time prep: ', stop - start)
    
    
# try differntem ethods

import asyncio

async def process_hdc_async(hdc):
    hdc = int(hdc)
    print(f"Processing HDC {hdc}")
    await regional_analysis(hdc)  # Replace with an async version of your function

async def main():
    hdcs_to_test = [39, 40]
    await asyncio.gather(*(process_hdc_async(hdc) for hdc in hdcs_to_test))

# Run the asyncio event loop
if __name__ == "__main__":
    asyncio.run(main())
    
