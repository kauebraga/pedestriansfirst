import warnings
import datetime
import os
import os.path
import gc
import numpy
import math
import statistics
import rasterstats
import rasterio
import rasterio.mask
import subprocess
import json
import traceback
import shutil
from tqdm import tqdm
#import utm_zone
import numpy as np
import osmnx as ox
import networkx as nx
import pandas as pd
import geopandas as gpd
import shapely.geometry
from shapely.geometry import LineString, Point
from shapely.ops import unary_union, transform
import shapely.ops
import topojson

from  funs.isochrones import *
from  funs.get_service_locations import *
from  funs.gtfs_parser import *
from  funs.prep_bike_osm import *
from  funs.prep_pop_ghsl import *
# import access
#import summarize_ttm

import pdb
import time
import re
import math



def make_patches(boundaries_latlon, crs_utm, patch_length = 50000, buffer = 500): #patch_length and buffer in m
    ''' 
    'tile' the boundaries of a city into patches, like a patchwork quilt, including a buffer
    buffer should be half the length of the longest PNX distance
    '''
    bounds_utm_poly = boundaries_latlon.to_crs(crs_utm).geometry.unary_union
    bbox_utm = bounds_utm_poly.bounds
    
    height_m = abs(bbox_utm[3]-bbox_utm[1])
    width_m = abs(bbox_utm[2]-bbox_utm[0])
    
    n_hslicers = math.floor(height_m / patch_length)
    n_vslicers = math.floor(width_m / patch_length)
    
    hslicers = []
    vslicers = []
    
    for i in range(1,n_hslicers+1):
        h_increment = height_m/(n_hslicers+1)
        lat = bbox_utm[1]+(i*h_increment)
        slicer = shapely.geometry.LineString([(bbox_utm[0],lat),(bbox_utm[2],lat)])
        hslicers.append(slicer)
        
    for i in range(1,n_vslicers+1):
        v_increment = width_m/(n_vslicers+1)
        lon = bbox_utm[0]+(i*v_increment)
        slicer = shapely.geometry.LineString([(lon,bbox_utm[1]),(lon, bbox_utm[3])])
        vslicers.append(slicer)
    
    
    patches = [bounds_utm_poly] #a list of polygons
    
    for slicer in hslicers+vslicers:
        newpatches = []
        for patch in patches:
            newpatches += list(shapely.ops.split(patch, slicer).geoms)
        patches = newpatches
    
    buffered_patches = []
    for patch in patches:
        buffered_patch = patch.buffer(buffer).intersection(bounds_utm_poly)
        buffered_patches.append(buffered_patch)
    patches_utm = gpd.GeoDataFrame(geometry=buffered_patches, crs=crs_utm)
    patches_latlon = patches_utm.to_crs(4326)
    patches_latlon['patch_id'] = patches_latlon.index
    unbuffered_patches_utm = gpd.GeoDataFrame(geometry=patches, crs=crs_utm)
    unbuffered_patches_latlon = unbuffered_patches_utm.to_crs(4326)
    unbuffered_patches_latlon['patch_id'] = patches_latlon.index
    
    print(f"cut {len(patches)} patches")
    
    return patches_latlon, unbuffered_patches_latlon

def cut(line, distance):
    # Cuts a line in two at a distance from its starting point
    #TODO: could I make this more efficient by taking the intersections of circles
    # with a radius of distance, distance*2, etc?
    #assuming that in most cases the line will be relatively straight.
    if distance <= 0.0 or distance >= line.length:
        return [LineString(line)]
    coords = list(line.coords)
    for i, p in enumerate(coords):
        pd = line.project(Point(p))
        if pd == distance:
            return [
                [LineString(coords[:i+1]),
                LineString(coords[i:])],
                pd]
        if pd > distance:
            cp = line.interpolate(distance)
            return [
                [LineString(coords[:i] + [(cp.x, cp.y)]),
                LineString([(cp.x, cp.y)] + coords[i:])],
                cp]

def get_line_mode(mode, name, agency, region, grade, brt_rating, current_year):
    regionalrail = pd.read_csv(f'input_data/transit_explorer/old/regionalrail.csv')
    regionalrail = regionalrail.loc[regionalrail.year_open <= current_year]
    itdp_mode = None
    if mode in ['Light Rail','Tramway']:
        itdp_mode = 'lrt'
    if mode in ['Light Metro','Metro']:
        itdp_mode = 'mrt'
    if mode == 'Regional Rail':
        if 'YES' in regionalrail[
                (regionalrail['name'] == name) & 
                (regionalrail['agency'] == agency) & 
                (regionalrail['region'] == region)]['ADD?'].values:
            itdp_mode = 'mrt'
    if mode == 'Bus Rapid Transit':
        # if brt_rating is not None:
            if brt_rating not in ['','Not BRT']:
                itdp_mode ='brt'
    grades = grade.split('; ')
    if 'at-grade' in grades or 'at grade' in grades:
        itdp_grade = 'atgrade'
    else:
        itdp_grade = 'gradesep'
    if itdp_mode:
        return itdp_mode + '_' + itdp_grade
        

def weighted_pop_density(array):
    total = 0
    for cell in array.compressed():
        total += cell**2
    return total / numpy.sum(array)



# boundaries= total_poly_latlon; name = analysis_areas.loc[0,'name']; boundary_buffer = 1000; id_code = hdc; patch_length = 50000; buffer_dist=100; ghsl_resolution = '1000'; headway_threshold=10

def spatial_analysis(boundaries, 
                      id_code,
                      name,
                      folder_name='', 
                      buffer_dist=100,#buffer out from nodes, m
                      headway_threshold=10,#min
                      to_test = [
                           'healthcare',
                           'schools',
                           'hs',
                           #'libraries',
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
                           #'journey_gap',
                           #'access',
                           #'transport_performance',
                           #'special',
                           ],
                      distances = { #network buffers, in meters
                            'healthcare': 1000,
                            'schools': 1000,
                            'libraries': 1000,
                            'bikeshare': 300,
                            'pnft': 500,
                            'pnrt': 1000,
                            'special': 250,
                            'pnpb': 250,
                            'pnab': 250,
                            'highways':500,
                            },
                      # years = [2023], #for PNRT and pop_dens. remember range(1,3) = [1,2]
                      years = [1975, 1980, 1985, 1990, 1995, 2000, 2005, 2010, 2015, 2020, 2022, 2023, 2024, 2025], #for PNRT and pop_dens. remember range(1,3) = [1,2]
                      current_year = 2023,
                      patch_length = 100000, #m
                      block_patch_length = 1000, #m
                      boundary_buffer = 1000, #m
                      blocks_simplification = 0.0001, #topo-simplification
                      services_simplification = 10, #m TODO replace with gpd simplification?
                      access_resolution = 2000, #m
                      transport_performance_speeds = { #make sure these align with the r script
                          'walk': 3.6, #km/h
                          'bike_lts1': 12,
                          'bike_lts2': 12,
                          #'transit': 12, #more arbitrary
                          },
                      transport_performance_times = [30,45,60], #mins
                      gtfs_files = [],
                      ghsl_projection = 'mw', #TODO make this do something
                      ghsl_resolution = '1000',
                      #max_edge_length = , #should not be bigger than 2xbuffer_dist
                      debug = True,
                      ):   
    
    dt = datetime.datetime.now()
    
    warnings.simplefilter(action='ignore', category=FutureWarning)
    warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

    # ox.config(use_cache=True, log_console=True, useful_tags_way=useful_tags, overpass_rate_limit=False, overpass_endpoint='https://overpass.kumi.systems/api')
    walk_filter = ('["area"!~"yes"]["highway"]["highway"!~"link|motor'
                   '|proposed|construction|abandoned'
                   '|platform|raceway"]'
                   '["service"!~"parking_aisle|driveway"]'
                   '["foot"!~"no"]["service"!~"private"]'
                   '{}').format(ox.settings.default_access) #used later
    
    if folder_name != '' and not folder_name[-1:] == '/':
        folder_name += '/'
    if 'hs' in to_test:
        for part in ['healthcare', 'schools']:
            if not part in to_test:
                to_test.append(part)
                    
    
    boundaries_latlon = gpd.GeoDataFrame(geometry = [boundaries])
    boundaries_latlon.crs = {'init':'epsg:4326'}
    longitude = round(numpy.mean(boundaries_latlon.geometry.centroid.x),10)
    utm_zone = int(math.floor((longitude + 180) / 6) + 1)
    utm_crs = '+proj=utm +zone={} +ellps=WGS84 +datum=WGS84 +units=m +no_defs'.format(utm_zone)
    boundaries_utm = boundaries_latlon.to_crs(utm_crs)
    boundaries_utm.geometry = boundaries_utm.geometry.buffer(boundary_buffer)
    boundaries_latlon = boundaries_utm.to_crs(epsg=4326)
    boundaries_mw = boundaries_utm.to_crs("ESRI:54009")
    boundaries = boundaries_latlon.geometry.unary_union
    
    print('Calculating geodata for Pedestrians First indicators in',name, "id",id_code)
    print('Measuring',str(to_test))
    
    # open population data
    for year in years:
        if year % 5 == 0:
            if year < current_year:
                in_file = f'input_data/ghsl_data_{ghsl_resolution}m_mw/GHS_POP_E{year}_GLOBE_R2023A_54009_{ghsl_resolution}/GHS_POP_E{year}_GLOBE_R2023A_54009_{ghsl_resolution}_V1_0.tif'
            else:
                in_file = f'input_data/ghsl_data_{ghsl_resolution}m_mw/GHS_POP_E{year}_GLOBE_R2023A_54009_{ghsl_resolution}/GHS_POP_E{year}_GLOBE_R2023A_54009_{ghsl_resolution}_V1_0.tif'
            with rasterio.open(in_file) as dataset:
                out_image, out_transform = rasterio.mask.mask(dataset, [boundaries_mw.unary_union], crop=True)
                out_meta = dataset.meta
                out_meta.update({"driver": "GTiff",
                      "height": out_image.shape[1],
                      "width": out_image.shape[2],
                      "transform": out_transform})
            if not os.path.exists(f"{folder_name}geodata/population"):
                os.mkdir(f"{folder_name}geodata/population")
            with rasterio.open(f"{folder_name}geodata/population/pop_{year}.tif", "w", **out_meta) as dest:
                dest.write(out_image)
            #for current year, 
    
    testing_services = []
    for service in ['healthcare', 'schools', 'libraries', 'bikeshare']:
        if service in to_test:
            testing_services.append(service)
            
    service_point_locations={}
    if len(testing_services) > 0:
        handler = ServiceHandler()
        # !!!!!!!!! AQUI LENTO!!!!!!!
        handler.apply_file(folder_name+f'temp/city_{current_year}.osm.pbf', locations=True)
        for service in testing_services:
          # service = testing_services[1]
            coords = handler.locationlist[service]
            service_point_locations[service] = gpd.GeoDataFrame(
                geometry = [Point(coord) for coord in coords],
                crs=4326)
        citywide_carfree = handler.carfreelist
                
    if 'pnab' in to_test or 'pnpb' in to_test:
        testing_services.append('pnab')
        testing_services.append('pnpb')
        
    if 'highways' in to_test:
        all_highway_lines = []
        all_highway_areas = []

    if 'special' in to_test:
        special = gpd.read_file(f'{folder_name}special.geojson')
        if special.geometry[0].type == 'MultiLineString':
            special.crs = 4326
            special_utm = special.to_crs(utm_crs)
            points=[]
            for line in special_utm.geometry.unary_union:
                line = shapely.ops.transform(lambda x, y, z=None: (x, y), line)
                while line.length > 50:
                    cutlines, point = cut(line, 50)
                    points.append(point)
                    line=cutlines[1]
            points_utm = gpd.GeoDataFrame(geometry=points, crs=utm_crs)
            points_latlon = points_utm.to_crs(4326)
            service_point_locations['special'] = points_latlon
        
        elif special.geometry[0].type == 'LineString':
            special.crs = 4326
            special_utm = special.to_crs(utm_crs)
            points=[]
            for line in special_utm.geometry:
                line = shapely.ops.transform(lambda x, y, z=None: (x, y), line)
                while line.length > 50:
                    cutlines, point = cut(line, 50)
                    points.append(point)
                    line=cutlines[1]
            points_utm = gpd.GeoDataFrame(geometry=points, crs=utm_crs)
            points_latlon = points_utm.to_crs(4326)
            service_point_locations['special'] = points_latlon
        elif special.geometry[0].type == 'Point':
            service_point_locations['special'] = special
        else:
            import pdb; pdb.set_trace()
            raise RuntimeError
        testing_services.append('special')

    if 'pnft' in to_test:
        testing_services.append('pnft')
        freq_stops, gtfs_wednesdays = get_frequent_stops(hdc = id_code, year = current_year, folder_name = folder_name, headwaylim = headway_threshold)
        service_point_locations['pnft'] = freq_stops
        gtfs_filenames = [file for file in os.listdir(folder_name+'temp/gtfs/') if file[-4:] == '.zip']
        
        
            
    if 'pnrt' in to_test:
        #get the dataitdp_modes = rt_lines.apply(lambda z: get_line_mode(z['mode'], z['name'], z['agency'], z['region'], z['grade'], z['brt_rating']), axis=1)        
        rt_lines = gpd.read_file(f'input_data/transit_explorer/lines.geojson')
        rt_stns = gpd.read_file(f'input_data/transit_explorer/stations.geojson')
        # filter only the transits for th eyear
        rt_lines = rt_lines.loc[rt_lines.year_open <= current_year]
        rt_stns = rt_stns.loc[rt_stns.year_open <= current_year]
        #geospatial clip to boundaries
        rt_lines = rt_lines.overlay(boundaries_latlon, how='intersection')
        rt_stns = rt_stns.overlay(boundaries_latlon, how='intersection')
        #remove lines/stations that are not public access
        rt_lines = rt_lines[rt_lines['limited'] == 0]
        rt_stns = rt_stns[rt_stns['limited'] == 0]
        
        rt_lines['rt_mode'] = None
        rt_stns['rt_mode'] = None
        
        #include only the modes we care about
        
        #itdp_modes = rt_lines.apply(lambda z: get_line_mode(z['mode'], z['name'], z['agency'], z['region'], z['grade'], z['brt_rating']), axis=1)        
        #rt_lines['rt_mode'] = itdp_modes
        for lineidx in rt_lines.index:
            # lineidx = 4
            itdp_mode = get_line_mode(rt_lines.loc[lineidx,'mode'], rt_lines.loc[lineidx,'name'], rt_lines.loc[lineidx,'agency'], rt_lines.loc[lineidx,'region'], rt_lines.loc[lineidx,'grade'], rt_lines.loc[lineidx,'brt_rating'], current_year)
            rt_lines.loc[lineidx,'rt_mode'] = itdp_mode
        
        rt_lines = rt_lines[rt_lines.rt_mode.isna() == False]
        if len(rt_lines) > 0:
            rt_lines_utm = ox.projection.project_gdf(rt_lines)
            
        if len(rt_stns) > 0:
            rt_stns_utm = ox.projection.project_gdf(rt_stns)
        
            for lineidx in rt_lines.index:
                selected_stns = rt_stns[(rt_stns_utm.intersects(rt_lines_utm.loc[lineidx,'geometry'].buffer(200))) & (rt_stns['mode'] == rt_lines.loc[lineidx, 'mode'])]
                rt_stns.loc[selected_stns.index,'rt_mode'] = rt_lines.loc[lineidx,'rt_mode']
        
        rt_stns = rt_stns[rt_stns.rt_mode.isna() == False]
            
        rt_isochrones = rt_stns.copy()
        rt_stns_utm = rt_stns.to_crs(utm_crs)
        rt_isochrones_utm = rt_isochrones.to_crs(utm_crs)
        
    # if 'journey_gap' in to_test and len(gtfs_filenames) > 0 and len(gtfs_wednesdays) > 0: #ie, it has GTFS
    #     journey_gap_success = access.journey_gap_calculations(
    #                 folder_name,
    #                 current_year,
    #                 boundaries_latlon,
    #                 gtfs_filenames,
    #                 gtfs_wednesdays,
    #                 access_resolution = access_resolution,
    #                 min_pop = 2000,
    #                 )
    
    if 'blocks' in to_test:
        outblocks = []
        block_counts = []
    
    quilt_isochrone_polys = {}
    for service in to_test:
        quilt_isochrone_polys[service] = False
        if 'pnab' in to_test or 'pnpb' in to_test:
            quilt_isochrone_polys['pnab'] = False
            quilt_isochrone_polys['pnpb'] = False
            quilt_allbike = gpd.GeoDataFrame(geometry=[], crs=utm_crs)
            quilt_protectedbike = gpd.GeoDataFrame(geometry=[], crs=utm_crs)
    
    
    # make patches
    patches, unbuffered_patches = make_patches(boundaries_latlon, utm_crs, patch_length=patch_length)
    unbuffered_patches_utm = unbuffered_patches.to_crs(utm_crs)
    if debug:
        patches.to_file(folder_name+'debug/patches.geojson', driver='GeoJSON')
    
    patch_times = []
    if len(to_test) > 0 and to_test != ["blocks"]:
        for p_idx in patches.index:
            # p_idx = 0
            patch = patches.loc[p_idx, 'geometry']
            unbuffered_patch = unbuffered_patches.loc[p_idx, 'geometry']
            unbuffered_patch_utm = unbuffered_patches_utm.loc[p_idx, 'geometry']
            try:
                patch_start = datetime.datetime.now()
                for cleanupfile in ['patchbounds.geojson',
                                    'patchbounds.poly',
                                    'patch.osm', 
                                    'allhwyspatch.osm']:
                                      # cleanupfile = 'pathbounds.geojson'
                    if os.path.exists(f'{folder_name}temp/'+cleanupfile):
                        os.remove(f'{folder_name}temp/'+cleanupfile)
                
                patchgdf = gpd.GeoDataFrame(geometry=[patch], crs=4326)
                
                patchgdf.to_file(f'{folder_name}temp/patchbounds.geojson', driver='GeoJSON')
                subprocess.run(f'python3 ogr2poly/ogr2poly.py {folder_name}temp/patchbounds.geojson > {folder_name}temp/patchbounds.poly', shell=True, check=True)
                time.sleep(2) # Sleep for 3 seconds
                
                # clip osm data to the patch boundaries?
                subprocess.check_call(['osmconvert',
                                       os.path.join(folder_name, f'temp/cityhighways_{current_year}.osm'),
                                       f'-B={folder_name}temp/patchbounds.poly',
                                       '--drop-broken-refs',  #was uncommented
                                       '--complete-ways',
                                       # '--complete-multipolygons',
                                       f'-o={folder_name}temp/patch_allroads.osm'])
                                       
                # subprocess.check_call('osmosis --read-xml file="cities_out/ghsl_region_2007/temp/cityhighways.osm" --bounding-polygon file="cities_out/ghsl_region_2007/temp/patchbounds.poly" --write-xml file="cities_out/ghsl_region_2007/temp/patch_allroads.osm"')
                # subprocess.check_call('osmosis --read-xml file="cities_out/ghsl_region_2007/temp/cityhighways.osm"')
                
                # !!!!!!!!!!!!
                # AQUI QUE TALVEZ ESTEJA DEMORANDO DEMAIS!!!
                print('Creating graph..')
                G_allroads_unsimplified = ox.graph_from_xml(f'{folder_name}temp/patch_allroads.osm', simplify=False, retain_all=True)
                # os.remove(f'{folder_name}temp/patch_allroads.osm')
            
                
                
                
                print('Filtering graph..')
                for x in G_allroads_unsimplified.edges:
                    if 'tunnel' not in G_allroads_unsimplified.edges[x].keys():
                        G_allroads_unsimplified.edges[x]['tunnel'] = "no" 
                        
                print('Lenght G_allroads_unsimplified:', {len(G_allroads_unsimplified)}) 
                        

                        
                        
                #then simplify
                G_allroads = ox.simplify_graph(G_allroads_unsimplified)
                
                print('Lenght G_allroads1:', {len(G_allroads)}) 
                
                                        
                # # Assuming G is your graph
                # num_nodes = G_allroads.number_of_nodes()
                # num_edges = G_allroads.number_of_edges()
                # nx.write_edgelist(G_allroads, f"teste_kaue/graph_{id_code}_simplified_pl.txt", data=False)  # Excludes edge attributes
                        
                # remove something?
                G_allroads.remove_nodes_from(list(nx.isolates(G_allroads)))
                
                print('Lenght G_allroads2:', {len(G_allroads)} ) 
                
                # num_nodes = G_allroads.number_of_nodes()
                # num_edges = G_allroads.number_of_edges()
                # nx.write_edgelist(G_allroads, f"teste_kaue/graph_{id_code}_removed_pl.txt", data=False)  # Excludes edge attributes
                

                
                # reproject
                print('Reprojecting graph..')
                if len(G_allroads.edges) > 0 and len(G_allroads.nodes) > 0:
                    G_allroads = ox.projection.project_graph(G_allroads, to_crs=utm_crs)
                    # export?
                print('Lenght G_allroads3:', {len(G_allroads)}) 
                
                # num_nodes = G_allroads.number_of_nodes()
                # num_edges = G_allroads.number_of_edges()
                # nx.write_edgelist(G_allroads, f"teste_kaue/graph_{id_code}_reproj_pl.txt", data=False)  # Excludes edge attributes

                    
                    
                # transform the network to gdfs: will be neccesary for process_bike, get_highways and blocks
                # THE PROBLEMS FROM THE PARALELIZION IS HERE!!!!! AND IT ONLY HAPPENS FOR EDGES!!!
                print("Transforming network to GDF")
                nodes, edges = ox.graph_to_gdfs(G_allroads)
                
                print('Lenght G_allroads3:', {len(edges)} ) 
                
                edges.to_file(f'teste_kaue/graph_{id_code}_reproj.geojson', driver='GeoJSON')
                
                
                    
                center_nodes = {}
                if 'pnab' in to_test or 'pnpb' in to_test:
                        print("Processing pnab/pnpb")
                  
                        for col in ['highway','cycleway','bicycle','cycleway:left','cycleway:right','cycleway:both']:
                          if not col in edges.columns:
                                edges[col] = ''
                                
                        tagged_cycleways = edges[(edges['highway'] == 'cycleway')]
                        cycle_paths = edges[(edges['highway'] == 'path') & (edges['bicycle'] == 'designated')]
                        on_street_tracks = edges[(edges['cycleway:right'] == 'track') |
                                                       (edges['cycleway:left'] == 'track') |
                                                       (edges['cycleway:both'] == 'track') |
                                                       (edges['cycleway'] == 'track') |
                                                       (edges['cycleway:right'] == 'opposite_track') |
                                                        (edges['cycleway:left'] == 'opposite_track') |
                                                        (edges['cycleway:both'] == 'opposite_track') |
                                                        (edges['cycleway'] == 'opposite_track')
                                                       ]
                        on_street_lanes = edges[(edges['cycleway:right'] == 'lane') |
                                                       (edges['cycleway:left'] == 'lane') |
                                                       (edges['cycleway:both'] == 'lane') |
                                                       (edges['cycleway'] == 'lane')]
                        
                        total_protectedbike = pd.concat([tagged_cycleways, cycle_paths, on_street_tracks])
                        total_allbike = pd.concat([total_protectedbike, on_street_lanes])
                        
                        
                        
                        
                        # exclude tiny, unconnected segments that aren't near larger ones
                        max_jump = 300
                        min_radius = 1000
                        
                        #remove isolated small lanes from protected network
                        total_protectedbike['in_real_network'] = "unknown"
                        for idx in total_protectedbike.index:
                            # idx = total_protectedbike.index[0]
                            already_identified = total_protectedbike.loc[idx,'in_real_network'] == "unknown"
                            if type(already_identified).__name__ == 'Series':
                                already_identified = already_identified.any()
                            if already_identified:
                                connected_indices = [idx]
                                if shapely.minimum_bounding_radius(total_protectedbike.loc[connected_indices,'geometry'].unary_union) > min_radius:
                                    total_protectedbike.loc[connected_indices,'in_real_network'] = "yes"
                                else:
                                    for i in range(0,10): #just so we don't end up in an infinite loop somehow
                                        connected_network = total_protectedbike.loc[connected_indices,'geometry'].unary_union
                                        # connected_network.to_file("teste1.shp")
                                        nearby = total_protectedbike[total_protectedbike.distance(connected_network) < max_jump]
                                        # nearby.to_file("teste1.shp")
                                        if 'yes' in nearby.in_real_network.unique():
                                            total_protectedbike.loc[connected_indices,'in_real_network'] = "yes"
                                            break
                                        if set(connected_indices) == set(nearby.index):
                                            if shapely.minimum_bounding_radius(total_protectedbike.loc[connected_indices,'geometry'].unary_union) > min_radius:
                                                total_protectedbike.loc[connected_indices,'in_real_network'] = "yes"
                                            else:
                                                total_protectedbike.loc[connected_indices,'in_real_network'] = "no"
                                            break
                                        else:
                                            connected_indices = list(nearby.index)
                                    if shapely.minimum_bounding_radius(total_protectedbike.loc[connected_indices,'geometry'].unary_union) > min_radius:
                                        total_protectedbike.loc[connected_indices,'in_real_network'] = "yes"
                        total_protectedbike = total_protectedbike[total_protectedbike.in_real_network == "yes"]
                        
                        #remove isolated small lanes from allbike network
                        total_allbike['in_real_network'] = "unknown"
                        for idx in total_allbike.index:
                            already_identified = total_allbike.loc[idx,'in_real_network'] == "unknown"
                            if type(already_identified).__name__ == 'Series':
                                already_identified = already_identified.any()
                            if already_identified:
                                connected_indices = [idx]
                                if shapely.minimum_bounding_radius(total_allbike.loc[connected_indices,'geometry'].unary_union) > min_radius:
                                    total_allbike.loc[connected_indices,'in_real_network'] = "yes"
                                else:
                                    for i in range(0,10): #just so we don't end up in an infinite loop somehow
                                        connected_network = total_allbike.loc[connected_indices,'geometry'].unary_union
                                        nearby = total_allbike[total_allbike.distance(connected_network) < max_jump]
                                        if 'yes' in nearby.in_real_network.unique():
                                            total_allbike.loc[connected_indices,'in_real_network'] = "yes"
                                            break
                                        if set(connected_indices) == set(nearby.index):
                                            if shapely.minimum_bounding_radius(total_allbike.loc[connected_indices,'geometry'].unary_union) > min_radius:
                                                total_allbike.loc[connected_indices,'in_real_network'] = "yes"
                                            else:
                                                total_allbike.loc[connected_indices,'in_real_network'] = "no"
                                            break
                                        else:
                                            connected_indices = list(nearby.index)
                                    if shapely.minimum_bounding_radius(total_allbike.loc[connected_indices,'geometry'].unary_union) > min_radius:
                                        total_allbike.loc[connected_indices,'in_real_network'] = "yes"
                        total_allbike = total_allbike[total_allbike.in_real_network == "yes"]
                                
                        quilt_allbike = pd.concat([quilt_allbike, total_allbike])
                        quilt_protectedbike = pd.concat([quilt_protectedbike, total_protectedbike])
                        
                        center_nodes['pnpb'] = set()
                        center_nodes['pnab'] = set()
                        for edge in total_protectedbike.index:
                            center_nodes['pnpb'].add(edge[0])
                            center_nodes['pnpb'].add(edge[1])
                        for edge in total_allbike.index:
                            center_nodes['pnab'].add(edge[0])
                            center_nodes['pnab'].add(edge[1])
                  
                  
                if 'highways' in to_test:
                    print("Processing highways...")
                    # SLOW HERE !!!!!!!!!!!!
                    highway_lines_gdf_ll = get_highways(nodes, edges)
                    # highway_lines_gdf_ll = get_service_locations.get_highways(G_allroads)
                    if highway_lines_gdf_ll is not None and len(list(highway_lines_gdf_ll.geometry)) > 0:
                        all_highway_lines += list(highway_lines_gdf_ll.geometry)
                        highway_lines_utm = ox.projection.project_gdf(highway_lines_gdf_ll)
                        highway_areas_utm = highway_lines_utm.buffer(distances['highways'])
                        highway_areas_ll = highway_areas_utm.to_crs(4326)
                        all_highway_areas +=list(highway_areas_ll.geometry)
                
                        
                    # if debug:
                    #     for node, data in G.nodes(data=True):
                    #         if 'osmid' in data:
                    #             data['osmid_original'] = data.pop('osmid')
                    #     ox.io.save_graph_geopackage(G, f'{folder_name}debug/{p_idx}_graph.gpkg')
                        
                print("Extracting nodes...")        
                for service in service_point_locations.keys():
                    if service in ['healthcare','schools','libraries','bikeshare','pnft','special']:
                      # service = 'schools'
                      # crop the points (schools hospitals etc) to the patch boundaries?
                        patch_points = service_point_locations[service].intersection(patch)
                        patch_points.crs=4326
                        patch_points_utm = patch_points.to_crs(utm_crs)
                        patch_points_utm = patch_points_utm[(~patch_points_utm.is_empty)
                                                            &(patch_points_utm.geometry!=None)]
                        if len(patch_points) > 0:
                            center_nodes[service]  = ox.distance.nearest_nodes(
                                G_allroads, 
                                patch_points_utm.geometry.x, 
                                patch_points_utm.geometry.y,
                                return_dist=False)
                    # if debug:
                    #     with open(f'{folder_name}debug/{p_idx}_centernodes.json', "w") as i :
                    #         json.dump(center_nodes, i)
                    
                    
                isochrone_polys = {}
                failures = {}
                for service in to_test:
                    failures[service] = 0
                    if 'pnab' in to_test or 'pnpb' in to_test:
                        failures['pnab']=0
                        failures['pnpb']=0
                    
                # Get polygons - isocrhones?
                for service in testing_services:
                    if service in center_nodes.keys():
                      if service in ['pnab', 'pnpb']:
                        # service = 'pnab'
                        print(f'getting polygons for {service}, {len(center_nodes[service])} center_nodes')
                        
                        # gdf = proper_iso_polys(
                        isochrone_polys[service] = proper_iso_polys(
                            G_allroads, 
                            center_nodes[service],
                            distance=distances[service],                                           
                            buffer=buffer_dist, 
                            infill=2500)
                        
                        # gdf_pnpb = gdf[gdf["id"].isin(center_nodes["pnpb"])]
                        # isochrone_polys["pnpb"] =  unary_union(gdf_pnpb["geometry"])
                        # isochrone_polys["pnab"] =  unary_union(gdf["geometry"])
                        
                        
                      else:
                        # service = 'pnab'
                        print(f'getting polygons for {service}, {len(center_nodes[service])} center_nodes')
                        
                        # gdf = proper_iso_polys(
                        isochrone_polys[service] = proper_iso_polys(
                            G_allroads, 
                            center_nodes[service],
                            distance=distances[service],                                           
                            buffer=buffer_dist, 
                            infill=2500)
                            
                        # isochrone_polys[service] = unary_union(gdf["geometry"])

                        
                for service in isochrone_polys.keys():
                    if service not in quilt_isochrone_polys.keys() or not quilt_isochrone_polys[service]:
                        quilt_isochrone_polys[service] = isochrone_polys[service]
                    elif isochrone_polys[service]:
                        quilt_isochrone_polys[service] = shapely.ops.unary_union([quilt_isochrone_polys[service],isochrone_polys[service]])
                    if debug == True:
                        if not os.path.exists(f'{folder_name}debug/patch{p_idx}'):
                            os.makedirs(f'{folder_name}debug/patch{p_idx}')
                        gpd.GeoSeries([isochrone_polys[service]], crs=utm_crs).to_crs(4326).to_file(f'{folder_name}debug/patch{p_idx}/{service}_latlon.geojson', driver='GeoJSON')
                    
                #get polygons for rapid transit
                if 'pnrt' in to_test:
                    for stn_idx in rt_stns.index:
                        if unbuffered_patch.contains(rt_stns.loc[stn_idx, 'geometry']):
                            print(f'getting polygons for pnrt')
                            stn_utm = rt_stns_utm.loc[stn_idx, 'geometry']
                            center_node = ox.distance.nearest_nodes(
                                G_allroads, 
                                stn_utm.x, 
                                stn_utm.y,
                                return_dist=False)
                            iso_poly = proper_iso_polys(
                                G_allroads, 
                                [center_node],
                                distance=distances['pnrt'],                                           
                                buffer=buffer_dist, 
                                infill=2500)
                            rt_isochrones_utm.loc[stn_idx,'geometry'] = iso_poly
                
                if 'blocks' in to_test:
                    if G_allroads and len(G_allroads.edges) > 0:
                        
                        streets = edges
                        streets = shapely.geometry.MultiLineString(list(streets.geometry))
                        merged = shapely.ops.linemerge(streets)
                        if merged:
                            borders = shapely.ops.unary_union(merged)
                            blocks = list(shapely.ops.polygonize(borders))
                            all_blocks = []
                            selected_areas = []
                            for block in blocks:
                                if 500 < block.area: #< 200000000:
                                    if block.interiors:
                                        block = shapely.geometry.Polygon(block.exterior)
                                    if block.centroid.within(unbuffered_patch_utm):
                                        area = round(block.area, 3)
                                        perim = round(block.length, 3)
                                        lemgth = round((perim * perim) / area, 3)
                                        if blocks_simplification:
                                            block = block.simplify(blocks_simplification)
                                        all_blocks.append((block, area, perim, lemgth))
                                        if (lemgth > 75) and (1000 > area or area > 1000000): #obvious outliers
                                            pass
                                        elif (lemgth > 30 ) and (3000 > area):
                                            pass
                                        else:
                                            selected_areas.append(area)
                            outblocks += all_blocks
                            print(f'cut {len(all_blocks)} blocks')
                            block_counts.append(len(all_blocks))
                        else:
                            block_counts.append(0)
                            print('not merged!')
                    else:
                        block_counts.append(0)
                        
                    
                del G_allroads_unsimplified
                del G_allroads
                gc.collect()
                    
                #import pdb;pdb.set_trace()
                patch_time = datetime.datetime.now() - patch_start
                    
                print(f"finished patch #{p_idx+1} out of {len(patches)} for {name} {id_code} in {patch_time}")
                patch_times.append(patch_time)
            except ox._errors.InsufficientResponseError:
                print('InsufficientResponseError')
                pass
            #except:
            #    print("GOT SOME ERROR FOR PATCH", p_idx)
            # except ValueError:
            #     print('ValueError')
            #     now = str(datetime.datetime.now())
            #     with open('error'+now+'.txt','w') as errout:
            #         traceback.print_exc(limit=3,file=errout)
            #     print('saved to error'+now+'.txt') 
    
        if debug:
            print('here')
            patches.times = patch_times
            patches.to_file(folder_name+'debug/patches_with_times.geojson', driver='GeoJSON')
            pd.DataFrame({'failures':failures}).to_csv(folder_name+'debug/failures.csv')
    
    # subprocess.run(f'echo -e "Finished patching" >> logs/running_{id_code}.csv')
    
    debugcounter = 1
    print(debugcounter); debugcounter+=1
    
    #start saving files
    geodata_subfolders = []
    for service in testing_services:
        geodata_subfolders.append(service+'/')
        geodata_subfolders.append(service+'_points/')
    geodata_subfolders += [
        'hs',
        'protectedbike/',
        'allbike/',
        'carfree/',
        'blocks/',
        'allhwys/',
        'buffered_hwys',
        ]
    for subfolder in geodata_subfolders:
        if not os.path.exists(f"{folder_name}geodata/{subfolder}/"):
            os.mkdir(f"{folder_name}geodata/{subfolder}/")
    
    print(debugcounter); debugcounter+=1
    
    for service in testing_services:
        
        print(debugcounter); debugcounter+=1
        print(service)
        if quilt_isochrone_polys[service]:
            service_utm = gpd.GeoDataFrame(geometry = [quilt_isochrone_polys[service]],
                                           crs=utm_crs)
            #TODO: check validity / all polygons?
            service_utm.geometry = service_utm.geometry.simplify(services_simplification)
            service_utm = gpd.overlay(service_utm ,boundaries_utm, how='intersection')
            service_latlon = service_utm.to_crs(epsg=4326)
            service_latlon.to_file(f"{folder_name}geodata/{service}/{service}_latlon_{current_year}.geojson", driver='GeoJSON')
        if service in service_point_locations.keys():
            service_point_locations[service].to_file(f"{folder_name}geodata/{service}_points/{service}_points_latlon_{current_year}.geojson", driver='GeoJSON')
          
    
    print(debugcounter); debugcounter+=1  
          
    if 'pnpb' in to_test or 'pnab' in to_test:
        if not quilt_protectedbike.empty:
            quilt_protectedbike = quilt_protectedbike.to_crs(4326)
            merged_protectedbike = quilt_protectedbike.intersection(boundaries)
            merged_protectedbike = gpd.GeoDataFrame(geometry = [merged_protectedbike.unary_union], crs=4326)
            merged_protectedbike.to_file(f"{folder_name}geodata/protectedbike/protectedbike_latlon_{current_year}.geojson",driver='GeoJSON')
        if not quilt_allbike.empty:
            quilt_allbike = quilt_allbike.to_crs(4326)
            merged_allbike = quilt_allbike.intersection(boundaries)
            merged_allbike = gpd.GeoDataFrame(geometry = [merged_allbike.unary_union], crs=4326)
            merged_allbike.to_file(f"{folder_name}geodata/allbike/allbike_latlon_{current_year}.geojson",driver='GeoJSON')

        
    print(debugcounter); debugcounter+=1
    
    if 'highways' in to_test:
        all_hwys_multiline = shapely.ops.unary_union(all_highway_lines)
        all_hwys_latlon = gpd.GeoDataFrame(geometry=[all_hwys_multiline], crs=4326)
        all_hwys_latlon.to_file(f"{folder_name}geodata/allhwys/allhwys_latlon_{current_year}.geojson",driver='GeoJSON')
        
        all_hwys_multipoly = shapely.ops.unary_union(all_highway_areas)
        all_hwy_buffs_latlon = gpd.GeoDataFrame(geometry=[all_hwys_multipoly], crs=4326)
        all_hwy_buffs_latlon.to_file(f"{folder_name}geodata/buffered_hwys/buffered_hwys_latlon_{current_year}.geojson",driver='GeoJSON')
        
        del all_hwys_latlon
        del all_hwys_multiline
        del all_hwys_multipoly
        del all_hwy_buffs_latlon
        
        gc.collect()
        
    print(debugcounter); debugcounter+=1
        
    if 'carfree' in to_test:
        carfree_latlon = gpd.GeoDataFrame(geometry = citywide_carfree)
        #just a latlon list of points
        carfree_latlon.crs = {'init':'epsg:4326'}
        carfree_utm = carfree_latlon.to_crs(utm_crs)
        carfree_utm.geometry = carfree_utm.geometry.buffer(100)
        #this is the analysis, the 100m buffer
        carfree_utm = gpd.GeoDataFrame(geometry = [shapely.ops.unary_union(carfree_utm.geometry)], crs=utm_crs)
        carfree_utm.geometry = carfree_utm.geometry.simplify(services_simplification)
        carfree_utm = gpd.overlay(carfree_utm ,boundaries_utm, how='intersection')
        carfree_latlon = carfree_utm.to_crs('epsg:4326')
        carfree_latlon.to_file(f"{folder_name}geodata/carfree/carfree_latlon_{current_year}.geojson",driver='GeoJSON')
       
    print(debugcounter); debugcounter+=1
        
    if 'hs' in to_test:
        if quilt_isochrone_polys['healthcare'] and quilt_isochrone_polys['schools']:
            service = 'hs'
            intersect = shapely.ops.unary_union([quilt_isochrone_polys['healthcare'].intersection(quilt_isochrone_polys['schools'])])
            if type(intersect) == shapely.geometry.collection.GeometryCollection:
                if not intersect.is_empty:
                    try:
                        intersect = [obj for obj in intersect if type(obj) == shapely.geometry.polygon.Polygon]
                    except TypeError: #intersect is a GeometryCollection
                        intersect = [obj for obj in intersect.geoms if type(obj) == shapely.geometry.polygon.Polygon]
                    intersect = shapely.geometry.MultiPolygon(intersect)
            hs_utm = gpd.GeoDataFrame(geometry = [intersect], crs=utm_crs)
            if hs_utm.geometry.area.sum() != 0:
                hs_utm = gpd.overlay(hs_utm ,boundaries_utm, how='intersection')
                hs_utm.geometry = hs_utm.geometry.simplify(services_simplification)
                hs_latlon = hs_utm.to_crs(epsg=4326)
                hs_latlon.to_file(f"{folder_name}geodata/{service}/{service}_latlon_{current_year}.geojson", driver='GeoJSON')
    
    print(debugcounter); debugcounter+=1
    
    if 'pnrt' in to_test:
        if not os.path.exists(folder_name+'geodata/rapid_transit/'):
            os.mkdir(folder_name+'geodata/rapid_transit/')
        if 'rt_mode' in rt_isochrones_utm.columns:
            rt_isochrones_latlon = rt_isochrones_utm.to_crs(4326)
            for year in years:
                if not os.path.exists(f'{folder_name}geodata/rapid_transit/{year}/'):
                    os.mkdir(f'{folder_name}geodata/rapid_transit/{year}/')
                #this could probably be more elegant
                mode_selectors = {
                    'brt_atgrade': rt_isochrones_latlon['rt_mode'] == 'brt_atgrade',
                    'brt_gradesep': rt_isochrones_latlon['rt_mode'] == 'brt_gradesep',
                    'brt': rt_isochrones_latlon['rt_mode'].isin(['brt_atgrade','brt_gradesep']),
                    'lrt_atgrade': rt_isochrones_latlon['rt_mode'] == 'lrt_atgrade',
                    'lrt_gradesep': rt_isochrones_latlon['rt_mode'] == 'lrt_gradesep',
                    'lrt': rt_isochrones_latlon['rt_mode'].isin(['lrt_atgrade','lrt_gradesep']),
                    'mrt_atgrade': rt_isochrones_latlon['rt_mode'] == 'mrt_atgrade',
                    'mrt_gradesep': rt_isochrones_latlon['rt_mode'] == 'mrt_gradesep',
                    'mrt': rt_isochrones_latlon['rt_mode'].isin(['mrt_atgrade','mrt_gradesep']),
                    'all_atgrade': rt_isochrones_latlon['rt_mode'].isin(['brt_atgrade','lrt_atgrade','mrt_atgrade']),
                    'all_gradesep': rt_isochrones_latlon['rt_mode'].isin(['brt_gradesep','lrt_gradesep','mrt_gradesep']),
                    'all': rt_isochrones_latlon['rt_mode'] != None,
                    }
                for mode in list(mode_selectors.keys()):
                    mode_selector = mode_selectors[mode]
                    opened_before = rt_isochrones_latlon['year_open'] <= year
                    not_closed = (np.isnan(rt_isochrones_latlon.year_clos) | (rt_isochrones_latlon.year_clos>year))
                    selector = mode_selector & opened_before & not_closed
                    total_isochrone = gpd.GeoDataFrame(
                        geometry=[rt_isochrones_latlon[selector].unary_union],
                        crs=4326)
                    
                    #fix clipping errors in total_isochone
                    if total_isochrone.unary_union and total_isochrone.unary_union.type == 'GeometryCollection':
                        new_geoms = [x for x in total_isochrone.unary_union.geoms if x.type in ['Polygon','MultiPolygon']]
                        total_isochrone = gpd.GeoDataFrame(geometry=new_geoms,crs=4326)
                    
                    total_isochrone.to_file(f'{folder_name}geodata/rapid_transit/{year}/{mode}_isochrones_ll.geojson', driver='GeoJSON')
                    select_stns = gpd.GeoDataFrame(
                        geometry=[rt_stns[selector].unary_union],
                        crs=4326)
                    select_stns.to_file(f'{folder_name}geodata/rapid_transit/{year}/{mode}_stations_ll.geojson', driver='GeoJSON')
                    
                line_mode_selectors = {
                    'brt_atgrade': rt_lines['rt_mode'] == 'brt_atgrade',
                    'brt_gradesep': rt_lines['rt_mode'] == 'brt_gradesep',
                    'brt': rt_lines['rt_mode'].isin(['brt_atgrade','brt_gradesep']),
                    'lrt_atgrade': rt_lines['rt_mode'] == 'lrt_atgrade',
                    'lrt_gradesep': rt_lines['rt_mode'] == 'lrt_gradesep',
                    'lrt': rt_lines['rt_mode'].isin(['lrt_atgrade','lrt_gradesep']),
                    'mrt_atgrade': rt_lines['rt_mode'] == 'mrt_atgrade',
                    'mrt_gradesep': rt_lines['rt_mode'] == 'mrt_gradesep',
                    'mrt': rt_lines['rt_mode'].isin(['mrt_atgrade','mrt_gradesep']),
                    'all_atgrade': rt_lines['rt_mode'].isin(['brt_atgrade','lrt_atgrade','mrt_atgrade']),
                    'all_gradesep': rt_lines['rt_mode'].isin(['brt_gradesep','lrt_gradesep','mrt_gradesep']),
                    'all': rt_lines['rt_mode'] != None,
                    }
                for mode in list(line_mode_selectors.keys()):
                    mode_selector = line_mode_selectors[mode]
                    opened_before = rt_lines['year_open'] <= year
                    not_closed = (np.isnan(rt_lines.year_clos) | (rt_lines.year_clos>year))
                    selector = mode_selector & opened_before & not_closed
                    select_lines = gpd.GeoDataFrame(
                        geometry=[rt_lines[selector].unary_union],
                        crs=4326)
                    select_lines.to_file(f'{folder_name}geodata/rapid_transit/{year}/{mode}_lines_ll.geojson', driver='GeoJSON')
               
        print(debugcounter); debugcounter+=1 
        if 'pnst' in to_test:
            if not os.path.exists(f'{folder_name}geodata/pnst/'):
                os.mkdir(f'{folder_name}geodata/pnst/')
                
            try:
                protectedbike = gpd.read_file(f"{folder_name}geodata/pnpb/pnpb_latlon_{current_year}.geojson")
                if protectedbike.unary_union is None:
                    protectedbike = gpd.GeoDataFrame(geometry = [], crs=4326)
            except:
                protectedbike = gpd.GeoDataFrame(geometry = [], crs=4326)
            
            try:
                rapidtransport = gpd.read_file(f'{folder_name}geodata/rapid_transit/{current_year}/all_isochrones_ll.geojson')
                # convert to polygon if empty?
                if rapidtransport["geometry"].is_empty.all():
                    rapidtransport["geometry"] = rapidtransport["geometry"].apply(lambda geom: Polygon() if geom.is_empty else geom)
                if rapidtransport.unary_union is None:
                    rapidtransport = gpd.GeoDataFrame(geometry = [], crs=4326)
            except:
                rapidtransport = gpd.GeoDataFrame(geometry = [], crs=4326)
            
            try:
                frequenttransport = gpd.read_file(f"{folder_name}geodata/pnft/pnft_latlon_{current_year}.geojson")
                if frequenttransport.unary_union is None:
                    frequenttransport = gpd.GeoDataFrame(geometry = [], crs=4326)
            except:
                frequenttransport = gpd.GeoDataFrame(geometry = [], crs=4326)
            
            if protectedbike.empty or protectedbike.unary_union is None: 
                #no bike
                transport_and_bike_latlon = gpd.GeoDataFrame(geometry = [], crs=4326)
            elif rapidtransport.empty and frequenttransport.empty:
                #bike, but neither kind of transit
                transport_and_bike_latlon = gpd.GeoDataFrame(geometry = [], crs=4326)
            elif rapidtransport.empty:
                #only frequent, no rapid
                transport_and_bike_latlon = frequenttransport.overlay(protectedbike, how="intersection")
            elif frequenttransport.empty:
                #only rapid, no frequent
                try:
                    transport_and_bike_latlon = rapidtransport.overlay(protectedbike, how="intersection")
                except TypeError:
                    newrt = gpd.GeoDataFrame(geometry = [x for x in rapidtransport.geometry[0].geoms if x.type in ['Polygon','MultiPolygon']], crs=rapidtransport.crs)
                    transport_and_bike_latlon = gpd.GeoDataFrame(geometry=newrt, crs=4326).overlay(protectedbike, how="intersection")
            else:
                #all of the above
                rapid_or_frequent = rapidtransport.overlay(frequenttransport, how="union")
                transport_and_bike_latlon = rapid_or_frequent.overlay(protectedbike, how="intersection")
                
                        
            
            transport_and_bike_utm = transport_and_bike_latlon.to_crs(utm_crs)
            if transport_and_bike_utm.geometry.area.sum() != 0:
                # I set keep_geom_type = False because it was returning an error as it resulted in GeometryCollection
                transport_and_bike_utm = gpd.overlay(transport_and_bike_utm, boundaries_utm, how='intersection', keep_geom_type = False)
                new_geoms = transport_and_bike_utm.geometry.simplify(services_simplification).make_valid().unary_union
                
                if new_geoms.type == 'GeometryCollection':
                    select_geoms = [x for x in new_geoms.geoms if x.type in ['Polygon','MultiPolygon']]
                    transport_and_bike_latlon= gpd.GeoDataFrame(geometry=select_geoms,crs=4326)
                
                transport_and_bike_latlon = transport_and_bike_utm.to_crs(epsg=4326)
                transport_and_bike_latlon.to_file(f"{folder_name}geodata/pnst/pnst_latlon_{current_year}.geojson", driver='GeoJSON') 
           
                
    print(debugcounter); debugcounter+=1
    
    if 'blocks' in to_test:
        #export all blocks
        blocks_utm = gpd.GeoDataFrame(geometry=[block[0] for block in outblocks], crs=utm_crs)
        blocks_utm['area_utm'] = [block[1] for block in outblocks]
        blocks_utm['perim'] = [block[2] for block in outblocks]
        blocks_utm['oblongness'] = [block[3] for block in outblocks]
        blocks_utm['density'] = [1000000/block[1] for block in outblocks]
        
        filtered_blocks_utm = blocks_utm[
            (blocks_utm.oblongness < 50) &
            (blocks_utm.area_utm > 1000) &
            (blocks_utm.area_utm < 1000000)]
        
        #TODO -- determine whether to normal simplify or toposimplify, and how much
        filtered_blocks_utm.geometry = filtered_blocks_utm.geometry.simplify(10)
        blocks_latlon = filtered_blocks_utm.to_crs(epsg=4326)
        #blocks_topo = topojson.Topology(blocks_latlon, prequantize=True)
        #blocks_latlon = blocks_topo.toposimplify(blocks_simplification).to_gdf()
        
        blocks_latlon.to_file(f"{folder_name}geodata/blocks/blocks_latlon_{current_year}.geojson", driver='GeoJSON')
        
        block_patches_latlon, block_unbuffered_patches_latlon = make_patches(
            boundaries_latlon, 
            utm_crs, 
            patch_length=block_patch_length
            )
        block_unbuf_patches_utm = block_unbuffered_patches_latlon.to_crs(utm_crs)
        #export            
        patch_densities = block_unbuffered_patches_latlon
        for patch_idx  in list(patch_densities.index):
            try:
                patch_densities.loc[patch_idx,'block_count'] = blocks_latlon.intersects(patch_densities.loc[patch_idx,'geometry'].centroid).value_counts()[True]
            except KeyError:
                patch_densities.loc[patch_idx,'block_count'] = 0 
        patch_densities_utm = patch_densities.to_crs(utm_crs)
        patch_densities_utm['density'] = patch_densities_utm.block_count / (patch_densities_utm.area / 1000000)
        patch_densities_latlon = patch_densities_utm.to_crs(epsg=4326)
        patch_densities_latlon.to_file(f"{folder_name}geodata/blocks/block_densities_latlon_{current_year}.geojson", driver='GeoJSON')

        
    #     print("getting blocks")
    #     #TODO -- should probably eventually fix this so that the patches
    #     #used in getting the blocks are not necessarily the same
    #     #as the ones used in summarizing them
    #     patches_latlon, unbuffered_patches_latlon = make_patches(
    #         boundaries_latlon, 
    #         utm_crs, 
    #         patch_length=block_patch_length
    #         )
    #     unbuf_patches_utm = unbuffered_patches_latlon.to_crs(utm_crs)
    #     print ("cut", len(list(unbuf_patches_utm)),"patches for block size in",name)
        
    #X     outblocks = []
    #X     block_counts = []
    #     for patch_idx in patches_latlon.index:
    #         patch_latlon = patches_latlon.loc[patch_idx, 'geometry']
    #         unbuf_patch_utm = unbuf_patches_utm.loc[patch_idx, 'geometry']
            
    #         print("patch"+str(patch_idx)+" of "+str(len(patches_latlon)), name )

    #         boundingarg = '-b='
    #         boundingarg += str(patch_latlon.bounds[0])+','
    #         boundingarg += str(patch_latlon.bounds[1])+','
    #         boundingarg += str(patch_latlon.bounds[2])+','
    #         boundingarg += str(patch_latlon.bounds[3])
            
    #         try:
    #             subprocess.check_call(['osmconvert', folder_name+'temp/citywalk.o5m', boundingarg, '--complete-ways', '--drop-broken-refs', '-o=patch.osm'])
    #             G = ox.graph_from_xml('patch.osm', simplify=True, retain_all=True)
    #         except ox._errors.InsufficientResponseError:
    #             G = False
    #             print("G=False")    
    #         except ValueError:
    #             try:
    #                 subprocess.check_call(['osmconvert', folder_name+'temp/citywalk.o5m', boundingarg, '--drop-broken-refs', '-o=patch.osm'])
    #                 G = ox.graph_from_xml('patch.osm', simplify=True, retain_all=True)
    #             except ox._errors.InsufficientResponseError:
    #                 G = False
    #                 print("G=False")    
    #             except ValueError:
    #                 try:
    #                     G = ox.graph_from_polygon(patch_latlon, 
    #                                               network_type='walk',
    #                                               #custom_filter=walk_filter, 
    #                                               simplify=False, 
    #                                               retain_all=True)
    #                 except ox._errors.InsufficientResponseError:
    #                     G = False
    #                     print("G=False")    
                    
                
    #         if G and len(G.edges) > 0:
    #             G = ox.project_graph(G, to_crs=utm_crs)
                
    #             streets = ox.utils_graph.graph_to_gdfs(G, nodes = False)
    #             streets = shapely.geometry.MultiLineString(list(streets.geometry))
    #             merged = shapely.ops.linemerge(streets)
    #             if merged:
    #                 borders = shapely.ops.unary_union(merged)
    #                 blocks = list(shapely.ops.polygonize(borders))
    #                 all_blocks = []
    #                 selected_areas = []
    #                 for block in blocks:
    #                     if 500 < block.area: #< 200000000:
    #                         if block.interiors:
    #                             block = shapely.geometry.Polygon(block.exterior)
    #                         if block.centroid.within(unbuf_patch_utm):
    #                             area = round(block.area, 3)
    #                             perim = round(block.length, 3)
    #                             lemgth = round((perim * perim) / area, 3)
    #                             if blocks_simplification:
    #                                 block = block.simplify(blocks_simplification)
    #                             all_blocks.append((block, area, perim, lemgth))
    #                             if (lemgth > 75) and (1000 > area or area > 1000000): #obvious outliers
    #                                 pass
    #                             elif (lemgth > 30 ) and (3000 > area):
    #                                 pass
    #                             else:
    #                                 selected_areas.append(area)
    #                 outblocks += all_blocks
    #                 print(f'cut {len(all_blocks)} blocks')
    #                 block_counts.append(len(all_blocks))
    #             else:
    #                 block_counts.append(0)
    #                 print('not merged!')
    #         else:
    #             block_counts.append(0)
        
    #     #export            
    #     patch_densities = unbuffered_patches_latlon.copy()
    #     patch_densities['block_count'] = block_counts
    #     patch_densities_utm = patch_densities.to_crs(utm_crs)
    #     patch_densities_utm['density'] = patch_densities_utm.block_count / (patch_densities_utm.area /1000000)
    #     patch_densities_latlon = patch_densities_utm.to_crs(epsg=4326)
    #     patch_densities_latlon.to_file(f"{folder_name}geodata/blocks/block_densities_latlon_{current_year}.geojson", driver='GeoJSON')
    #     blocks_utm = gpd.GeoDataFrame(geometry=[block[0] for block in outblocks], crs=utm_crs)
    #     blocks_utm['area_utm'] = [block[1] for block in outblocks]
    #     blocks_utm['perim'] = [block[2] for block in outblocks]
    #     blocks_utm['oblongness'] = [block[3] for block in outblocks]
    #     blocks_utm['density'] = [1000000/block[1] for block in outblocks]
        
    #     filtered_blocks_utm = blocks_utm[
    #         (blocks_utm.oblongness < 50) &
    #         (blocks_utm.area_utm > 1000) &
    #         (blocks_utm.area_utm < 1000000)]
        
    #     #TODO -- determine whether to normal simplify or toposimplify, and how much
    #     filtered_blocks_utm.geometry = filtered_blocks_utm.geometry.simplify(10)
    #     blocks_latlon = filtered_blocks_utm.to_crs(epsg=4326)
    #     #blocks_topo = topojson.Topology(blocks_latlon, prequantize=True)
    #     #blocks_latlon = blocks_topo.toposimplify(blocks_simplification).to_gdf()
        
    #     blocks_latlon.to_file(f"{folder_name}geodata/blocks/blocks_latlon_{current_year}.geojson", driver='GeoJSON')
    
    
    ft = datetime.datetime.now()
    return ft-dt
    

def people_near_x(service_gdf_utm, folder_name, boundaries_utm, year, sqkm_per_pixel):
    if service_gdf_utm is None:
        return 0
    if len(service_gdf_utm) > 1:
        try:
            service_gdf_utm = gpd.GeoDataFrame(geometry = [service_gdf_utm.unary_union], crs = service_gdf_utm.crs)
        except:
            import pdb; pdb.set_trace()
        
    service_gdf_utm.geometry = service_gdf_utm.geometry.make_valid()
    sel_service_utm = service_gdf_utm.intersection(boundaries_utm)
    sel_service_mw = sel_service_utm.to_crs('ESRI:54009')
    service_area = sel_service_utm.area.sum()
    if service_area == 0:
        total_PNS = 0
    else:
        modulo = year % 5
        earlier = year - modulo
        later = year + (5 - modulo)
        earlier_stats = rasterstats.zonal_stats(
            sel_service_mw,
            f"{folder_name}geodata/population/pop_{earlier}.tif", 
            stats=['mean'], 
            all_touched=True
            ) 
        earlier_mean = earlier_stats[0]['mean']
        if earlier_mean is None:
            return 0
        earlier_dens = earlier_mean / sqkm_per_pixel 
        if modulo > 0:
            try:
                later_stats = rasterstats.zonal_stats(
                    sel_service_mw,
                    f"{folder_name}geodata/population/pop_{later}.tif", 
                    stats=['mean'], 
                    all_touched=True
                    ) 
                
                later_mean = later_stats[0]['mean']
            except rasterio.errors.RasterioIOError:
                later_mean = earlier_mean
            if later_mean is None:
                return 0
            later_dens = later_mean / sqkm_per_pixel 
            peryear_diff = (later_dens - earlier_dens) / 5
            mean_dens_per_m2 = (earlier_dens + (modulo * peryear_diff)) / 1000000 #km to m
        else:
            mean_dens_per_m2 = earlier_dens / 1000000 #km to m
        total_PNS = mean_dens_per_m2 * service_area
        return total_PNS




def calculate_indicators(analysis_areas, 
                      folder_name='', 
                      to_test = [
                           'healthcare',
                           'schools',
                           'hs',
                           #'libraries',
                           'bikeshare',
                           'carfree',
                           'blocks',
                           'density',
                           'pnft',
                           'pnrt',
                           'pnpb', #protected bikeways
                           'pnab', #all bikeways
                           'pnst',
                           'highways',
                           #'special',
                           #'transport_performance',
                           #'connectome',
                           'journey_gap',
                           ],
                      #years = range(1975,2031),
                      years = [1975, 1980, 1985, 1990, 1995, 2000, 2005, 2010, 2015, 2020, 2023, 2024, 2025],
                      current_year = 2024,
                      ghsl_resolution = '1000',
                      debug = True,
                      ):   

    if folder_name != '' and not folder_name[-1:] == '/':
        folder_name += '/'
        
        
    #to see if we should put NA instead of 0 for pnft and journey_gap
    if 'pnft' in to_test or 'journey gap' in to_test:
        try:
            gtfs_filenames = os.listdir(folder_name+'temp/gtfs/')
        except:
            gtfs_filenames = []
    
    analysis_areas_utm = ox.projection.project_gdf(analysis_areas)
    utm_crs = analysis_areas_utm.crs
    analysis_areas_mw = analysis_areas.to_crs("ESRI:54009")
    
    sqkm_per_pixel = (float(ghsl_resolution) / 1000) ** 2
    
    # 1. Load data files for each indicator
    # 1.1: Services (People Near X, except for rapid transit)
    services = ['healthcare','schools','hs','libraries','bikeshare','pnab','pnpb',
                'pnft','pnst','carfree','special', 'highways']
    service_gdfs_utm = {}
    for service in services:
        if service in to_test:
            if service == "highways":
                geodata_path = f"{folder_name}geodata/buffered_hwys/buffered_hwys_latlon_{current_year}.geojson"
            else:
                geodata_path = f'{folder_name}geodata/{service}/{service}_latlon_{current_year}.geojson'
            
            if os.path.exists(geodata_path):
                service_gdfs_utm[service] = gpd.read_file(geodata_path).to_crs(utm_crs)
            else:
                service_gdfs_utm[service] = None
    
    service_points_ll = {}     
    for service_with_points in ['healthcare', 'schools', 'libraries', 'bikeshare', 'pnft','special',]:
        if service_with_points in to_test:
            geodata_path = f"{folder_name}geodata/{service_with_points}_points/{service_with_points}_points_latlon_{current_year}.geojson"
            service_points_ll[service_with_points] = gpd.read_file(geodata_path)
    
    # 1.2 Bikeways
    if 'pnab' in to_test:
        filename = f"{folder_name}geodata/allbike/allbike_latlon_{current_year}.geojson"
        if os.path.exists(filename):
            all_bikeways_utm = gpd.read_file(filename).to_crs(utm_crs)
        else:
            all_bikeways_utm = None
    if 'pnpb' in to_test:
        filename = f"{folder_name}geodata/protectedbike/protectedbike_latlon_{current_year}.geojson"
        if os.path.exists(filename):
            protected_bikeways_utm = gpd.read_file(filename).to_crs(utm_crs)
        else:
            protected_bikeways_utm = None
            
    # 1.3 PNRT
    if 'pnrt' in to_test:
        geodata_path = f'{folder_name}geodata/rapid_transit/{current_year}/all_isochrones_ll.geojson'
        has_rt = False
        if os.path.exists(geodata_path):
            has_rt = True
            modes = ['all','all_atgrade','all_gradesep','mrt','mrt_atgrade','mrt_gradesep','lrt','lrt_atgrade','lrt_gradesep','brt','brt_atgrade','brt_gradesep',]
            rt_isochrones = {}
            rt_lines = {}
            rt_stns = {}
            for mode in modes:
                rt_isochrones[mode] = {}
                rt_lines[mode] = {}
                rt_stns[mode] = {}
                for year in years:
                    iso_path = f'{folder_name}geodata/rapid_transit/{year}/{mode}_isochrones_ll.geojson'
                    if os.path.exists(iso_path):
                        iso_utm = gpd.read_file(iso_path).to_crs(utm_crs)
                        rt_isochrones[mode][year] = iso_utm
                    else:
                        rt_isochrones[mode][year] = None
                    lines_path = f'{folder_name}geodata/rapid_transit/{year}/{mode}_lines_ll.geojson'
                    if os.path.exists(lines_path):
                        lines_utm = gpd.read_file(lines_path).to_crs(utm_crs)
                        rt_lines[mode][year] = lines_utm
                    else:
                        rt_lines[mode][year] = None
                    stns_path = f'{folder_name}geodata/rapid_transit/{year}/{mode}_stations_ll.geojson'
                    if os.path.exists(stns_path):
                        stn_utm = gpd.read_file(stns_path).to_crs(utm_crs)
                        rt_stns[mode][year] = stn_utm
                    else:
                        rt_stns[mode][year] = None
    
    # 1.4 Blocks
    if 'blocks' in to_test:
        geodata_path = f"{folder_name}geodata/blocks/blocks_latlon_{current_year}.geojson"
        if os.path.exists(geodata_path):
            blocks = gpd.read_file(geodata_path)
        else:
            blocks = None
            
            
    # if 'journey_gap' in to_test:
    #     geodata_path = f'{folder_name}geodata/access/grid_pop_evaluated_{current_year}.geojson'
    #     if os.path.exists(geodata_path):
    #         access_grid = gpd.read_file(geodata_path)
    #     else:
    #         access_grid = None
    
    # 2. Iterate through analysis_areas gdf, calculate all indicators
    # idx = 202
    for idx in analysis_areas.index:
        # idx = 4
        try:
            print('getting results for', analysis_areas.loc[idx, 'name'])
            
            analysis_areas.loc[idx, f'has_gtfs'] = (len(gtfs_filenames) > 0)
            
            boundaries_mw = analysis_areas_mw.loc[idx,'geometry']
            boundaries_utm = analysis_areas_utm.loc[idx,'geometry']
            boundaries_ll = analysis_areas.loc[idx,'geometry']
            
            
            analysis_areas.loc[idx, f'area'] = boundaries_utm.area
            
            # 2.1 Get total_pop for each area, so that we can measure PNx.
            #     Get density at the same time for convenience.
            # 2.1.1 First do years where we have GHSL data...
            # year = 2024
            for year in years:
                if (year % 5) == 0:
                    # idx = 138
                    # year = 2025
                    pop_stats = rasterstats.zonal_stats(
                        analysis_areas_mw.loc[idx,'geometry'],
                        f"{folder_name}geodata/population/pop_{year}.tif", 
                        stats=['mean'],
                        all_touched=True
                        ) 
                    mean_density_per_km2 = pop_stats[0]['mean'] / sqkm_per_pixel
                    mean_density_per_m2 = mean_density_per_km2 / 1000000
                    total_pop = mean_density_per_m2 * boundaries_utm.area
                    analysis_areas.loc[idx, f'total_pop_{year}'] = total_pop
                    
                    density = rasterstats.zonal_stats(boundaries_mw, 
                                            f"{folder_name}geodata/population/pop_{year}.tif", 
                                            stats = [],
                                            all_touched = True,
                                            add_stats={'weighted': weighted_pop_density}
                                            )[0]['weighted']
                    analysis_areas.loc[idx, f'density_{year}'] = density / sqkm_per_pixel 
            # 2.1.2 ...then interpolate other years. 
            #       The largest and smallest years must be in GHSL (divisible by 5)
            for year in years:
                if (year % 5) != 0:
                    earlier = year - (year % 5)
                    later = year + (5 - (year % 5))
                    if later > max(years):
                        later = earlier
                    earlier_pop = analysis_areas.loc[idx, f'total_pop_{earlier}']
                    later_pop = analysis_areas.loc[idx, f'total_pop_{later}']
                    peryear_diff_pop = (later_pop - earlier_pop) / 5
                    total_pop = earlier_pop + ((year % 5) * peryear_diff_pop)
                    analysis_areas.loc[idx, f'total_pop_{year}'] = total_pop
                    
                    earlier_dens = analysis_areas.loc[idx, f'density_{earlier}']
                    later_dens = analysis_areas.loc[idx, f'density_{later}']
                    peryear_diff_dens = (later_dens - earlier_dens) / 5
                    current_dens = earlier_dens + ((year % 5) * peryear_diff_dens)
                    analysis_areas.loc[idx, f'density_{year}'] = current_dens
                        
            # 2.2 People Near Services
            services = ['healthcare','schools','hs','libraries','bikeshare','pnab','pnpb',
                        'pnft','pnst','carfree','highways','special']
            for service in services:
                if service in to_test:
                    # service = "pnpb"
                    total_PNS = people_near_x(
                        service_gdfs_utm[service],
                        folder_name, 
                        boundaries_utm, 
                        current_year, 
                        sqkm_per_pixel)
                    if total_PNS is not None:
                        perc_PNS = total_PNS / analysis_areas.loc[idx,f'total_pop_{current_year}']
                        perc_PNS = min(perc_PNS, 1)
                        analysis_areas.loc[idx,f'{service}_{current_year}'] = perc_PNS
                    else:
                        analysis_areas.loc[idx,f'{service}_{current_year}'] = 0
                        
            for service_with_points in ['healthcare', 'schools', 'libraries', 'bikeshare', 'pnft','special',]:
                if service_with_points in to_test:
                    total_services = service_points_ll[service_with_points].intersects(boundaries_ll)
                    try:
                        analysis_areas.loc[idx,f'n_points_{service_with_points}_{current_year}'] = total_services.value_counts()[True]
                    except KeyError:
                        analysis_areas.loc[idx,f'n_points_{service_with_points}_{current_year}']  = 0
    
                
            if len(gtfs_filenames) == 0:
                analysis_areas.loc[idx,f'pnft_{current_year}'] = "NA"
                analysis_areas.loc[idx,f'n_points_pnft_{current_year}'] = "NA"
                
                
            # 2.2.1 HIGHWAYS ARE SPECIAL
            if 'highways' in to_test:
                PNNH = 1 - analysis_areas.loc[idx,f'highways_{current_year}']
                analysis_areas.loc[idx,f'people_not_near_highways_{current_year}'] = PNNH
                if service_gdfs_utm['highways'] is not None:
                    selected_highways_gdf_utm = service_gdfs_utm['highways'].intersection(boundaries_utm)
                    hwy_m = sum(selected_highways_gdf_utm.geometry.length) 
                else:
                    hwy_m = 0
                analysis_areas.loc[idx,f'highway_km_{current_year}'] = hwy_m / 1000
                
            # 2.3 People Near Bikeways
            if 'pnab' in to_test:
                if all_bikeways_utm is not None:
                    selected_all_bikeways_utm = all_bikeways_utm.intersection(boundaries_utm)
                    unprotected_m = sum(selected_all_bikeways_utm.geometry.length)
                else:
                    unprotected_m = 0
                analysis_areas.loc[idx,f'all_bikeways_km_{current_year}'] = unprotected_m / 1000
                
            if 'pnpb' in to_test:
                if protected_bikeways_utm is not None:
                    selected_protected_bikeways_utm = protected_bikeways_utm.intersection(boundaries_utm)
                    protected_m = sum(selected_protected_bikeways_utm.geometry.length)
                else:
                    protected_m = 0
                analysis_areas.loc[idx,f'protected_bikeways_km_{current_year}'] = protected_m / 1000
            
                
            if 'pnrt' in to_test and has_rt: 
                for mode in ['brt','lrt','mrt','all']:
                    check_iso = rt_isochrones[mode][current_year]
                    #short-circuit?
                    if (check_iso is None) or (check_iso.intersection(boundaries_utm).unary_union is None):
                        for year in years:
                            analysis_areas.loc[idx,f'PNrT_{mode}_{year}'] = 0
                            analysis_areas.loc[idx,f'km_{mode}_{year}'] = 0
                            analysis_areas.loc[idx,f'stns_{mode}_{year}'] = 0
                            analysis_areas.loc[idx,f'rtr_{mode}_{year}'] = 0
                    else:
                        for year in years:
                            if year <= current_year:
                                #PNRT
                                isochrones_utm = rt_isochrones[mode][year]
                                if isochrones_utm is None:
                                    analysis_areas.loc[idx,f'PNrT_{mode}_{year}'] = 0
                                else:
                                    total_pnrt = people_near_x(
                                            isochrones_utm,
                                            folder_name, 
                                            boundaries_utm, 
                                            current_year, 
                                            sqkm_per_pixel)
                                    if total_pnrt is None:
                                        analysis_areas.loc[idx,f'PNrT_{mode}_{year}'] = 0
                                    else:
                                        pnrt = total_pnrt / analysis_areas.loc[idx,f'total_pop_{year}']
                                        analysis_areas.loc[idx,f'PNrT_{mode}_{year}'] = pnrt
                                # kms of line, stations, RTR
                                
                                lines_utm = rt_lines[mode][year]
                                if lines_utm is None:
                                    km = 0
                                else:
                                    lines_utm = lines_utm.intersection(boundaries_utm)
                                    km = sum(lines_utm.geometry.length) / 1000
                                    
                                stns_utm = rt_stns[mode][year]
                                if stns_utm is None:
                                    n_stns= 0
                                else:
                                    try:
                                        n_stns = stns_utm.intersects(boundaries_utm).value_counts()[True]
                                    except KeyError:
                                        n_stns = 0
                                
                                analysis_areas.loc[idx,f'km_{mode}_{year}'] = km
                                analysis_areas.loc[idx,f'stns_{mode}_{year}'] = n_stns
                                analysis_areas.loc[idx,f'rtr_{mode}_{year}'] = km / (analysis_areas.loc[idx,f'total_pop_{year}']/1000000)
          
            if 'blocks' in to_test:
                if blocks is not None:
                    try:
                        selection = blocks[blocks.intersects(boundaries_ll)]
                        av_size = selection.area_utm.mean()
                        block_density = 1000000 / av_size
                    except: 
                        block_density = 'ERROR'
                else:
                    block_density = 'NA'
                analysis_areas.loc[idx,f'block_density_{current_year}'] = block_density
                    
            # if 'journey_gap' in to_test:
            #     if access_grid is not None:
            #         grid_overlap = access_grid[access_grid.intersects(boundaries_ll)]
            #         area_pop = grid_overlap.population.sum()
                    
            #         journey_gap_weighted_total = grid_overlap.journey_gap_weighted.sum()
            #         journey_gap = journey_gap_weighted_total / area_pop
            #         analysis_areas.loc[idx,f'journey_gap_{current_year}'] = journey_gap
                    
                if len(gtfs_filenames) == 0:
                    analysis_areas.loc[idx,f'cumsum_journeygap_{current_year}'] = "NA"
                    analysis_areas.loc[idx,f'time_journeygap_{current_year}'] = "NA"
                    analysis_areas.loc[idx,f'grav_journeygap_{current_year}'] = "NA"
        except TypeError:
            pass
