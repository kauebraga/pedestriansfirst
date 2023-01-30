import requests
import json
import fiona
import osmnx as ox
import numpy as np
import geopandas as gpd
import shapely
from shapely.geometry import Point

import osmium
import shapely.wkb

def get_highways(simple_projected_G,
                 min_length = 1500, #meters
                 ):
    #get the car-only network and the links of major (divided) roads
    nodes, edges = ox.graph_to_gdfs(simple_projected_G)
    car_tags = ['motorway','trunk','primary','secondary','tertiary','unclassified',
                'residential','living_street','service','road']
    for tag in car_tags.copy():
        car_tags.append(tag+'_link')
    major_tags = ['motorway','trunk','primary','secondary']
    car_roads = edges[edges.highway.isin(car_tags)]
    multi_car_G = ox.graph_from_gdfs(nodes, car_roads)
    major_roads = edges[(edges.highway.isin(major_tags)) & (edges.oneway == True)].copy()
    #only include major roads with at least 2 lanes per direction
    if 'lanes' in major_roads.columns:
        for idx in major_roads.index:
            lanes = major_roads.loc[idx, 'lanes']
            if type(lanes) == type('string'):
                lanes = lanes.split(';')
            if type(lanes) == type([]):
                all_lanes = []
                for lane_ct in lanes:
                    all_lanes += lane_ct.split(';')
                lanes = min(lane_ct)
            lanes = float(lanes)
            if np.isnan(lanes):
                lanes = 3 #if the number of lanes isn't given, we assume it's more than 2 per direction
            if lanes < 2:
                major_roads.drop(idx, inplace=True)
    if len(major_roads) == 0:
        return None
            
    # Identify all the nodes with no more than three neighbors 
    # ie, exclude four-way intersections
    major_nodes = set()
    for idx in major_roads.index:
        major_nodes.add(idx[0])
        major_nodes.add(idx[1])
    grade_separated_nodes = []
    at_grade_nodes = []
    for center_node in major_nodes:
        neighbors = set()
        for edge in list(multi_car_G.out_edges(center_node)):
            for neighbor in edge:
                neighbors.add(neighbor)
        for edge in list(multi_car_G.in_edges(center_node)):
            for neighbor in edge:
                neighbors.add(neighbor)
        if center_node in neighbors:
            neighbors.remove(center_node)
        if len(neighbors) > 3:
            at_grade_nodes.append(center_node)
        else:
            grade_separated_nodes.append(center_node)
    # find all the places with at least 1km between 4-way intersections
    separation_breakers = nodes.loc[at_grade_nodes]
    separation_break_poly = separation_breakers.buffer(2).unary_union
    #major_roads_multiline = major_roads.unary_union
    roads_poly = major_roads.buffer(0.5).unary_union
    roads_poly_diff = roads_poly.difference(separation_break_poly)
    if roads_poly_diff is None:
        return None
    roads_poly_gdf = gpd.GeoDataFrame(crs = edges.crs, geometry = list(roads_poly_diff.geoms))
    
    #cut out polys that are too short
    for idx in roads_poly_gdf.index:
        maxdist = 0
        poly = roads_poly_gdf.loc[idx, 'geometry'].convex_hull
        for a in poly.exterior.coords:
            for b in poly.exterior.coords:
                dist = Point(a).distance(Point(b))
                if dist > maxdist:
                    maxdist = dist
        if maxdist < min_length:
            roads_poly_gdf.drop(idx, inplace=True)
        
    return roads_poly_gdf.unary_union
    
def bbox_from_shp(file_loc):
    with fiona.open(file_loc,'r') as source: 
        bbox = (source[0]['properties']['BBX_LATMN'],
               source[0]['properties']['BBX_LONMN'],
               source[0]['properties']['BBX_LATMX'],
               source[0]['properties']['BBX_LONMX'],)
    #TODO: calculate bounding box from shape instead of referencing variables
    return bbox

queries = {

'healthcare' : '''
[timeout:900][maxsize:1073741824][out:json];
(
node
["amenity"~"hospital|doctors|clinic|pharmacy"]
(poly:"{poly}");
node
["healthcare"~"alternative|birthing_center|centre|midwife|nurse|hospital|doctor|clinic|pharmacy|yes"]
(poly:"{poly}");
way
["amenity"~"hospital|doctors|clinic|pharmacy"]
(poly:"{poly}");
way
["healthcare"~"alternative|birthing_center|centre|midwife|nurse|hospital|doctor|clinic|pharmacy|yes"]
(poly:"{poly}");
);
(._;);
out skel center qt;
''',

'schools' : '''
[timeout:900][maxsize:1073741824][out:json];
(
node
["amenity"~"school|kindergarten"]
(poly:"{poly}");
node
["school"]
 (poly:"{poly}");
way
["amenity"~"school|kindergarten"]
(poly:"{poly}");
way
["school"]
(poly:"{poly}");
rel
["amenity"~"school|kindergarten"]
(poly:"{poly}");
rel
["school"]
(poly:"{poly}");
);
(._;);
out skel center qt;
''',

'libraries' : '''
[timeout:900][maxsize:1073741824][out:json];
(
node
["amenity"~"library|bookcase"]
(poly:"{poly}");
way
["amenity"~"library|bookcase"]
(poly:"{poly}");
);
(._;);
out skel center qt;
''',
}

categories = {
        'healthcare':[],
        'schools':[],
        'libraries':[]
        }

wkbfab = osmium.geom.WKBFactory()
    
class ServiceHandler(osmium.SimpleHandler): #newer
    def __init__(self):
        super().__init__()
        self.locationlist = {
                'healthcare':[],
                'libraries':[],
                'schools':[],
                'bikeshare':[],
                }
        self.carfreelist = []
        

    def node(self, n):
        if 'amenity' in n.tags and n.tags['amenity'] in ['library','bookcase']:
            self.locationlist['libraries'].append((n.location.lon, n.location.lat))
        
        if ( ('amenity' in n.tags and 
               n.tags['amenity'] in ['school','kindergarten']) or
             ('school' in n.tags) ):
            self.locationlist['schools'].append((n.location.lon, n.location.lat))
            
        if ( ('amenity' in n.tags and 
               n.tags['amenity'] in ['hospital','doctors','clinic','pharmacy']) or
             ('healthcare' in n.tags and 
               n.tags['healthcare'] in ['alternative','birthing_center','centre','midwife','nurse','hospital','doctor','clinic','pharmacy','yes']) ):
            self.locationlist['healthcare'].append((n.location.lon, n.location.lat))

        if ( ('amenity' in n.tags and 
               n.tags['amenity'] in ['bicycle_rental']) or
             ('bicycle_rental' in n.tags) ):
            if 'bicycle_rental' in n.tags:
                if not n.tags['bicycle_rental'] in ['shop']:
                    self.locationlist['bikeshare'].append((n.location.lon, n.location.lat))
            else:
                self.locationlist['bikeshare'].append((n.location.lon, n.location.lat))
                

    def area(self, a):
        try:
            if 'amenity' in a.tags and a.tags['amenity'] in ['library','bookcase']:
                    wkb = wkbfab.create_multipolygon(a)
                    poly = shapely.wkb.loads(wkb, hex=True)
                    centroid = poly.representative_point()
                    self.locationlist['libraries'].append((centroid.x, centroid.y))
                
            if 'amenity' in a.tags and a.tags['amenity'] in ['bicycle_rental']:
                    if 'bicycle_rental' in a.tags:
                        if a.tags['bicycle_rental'] in ['shop']:
                            wkb = wkbfab.create_multipolygon(a)
                            poly = shapely.wkb.loads(wkb, hex=True)
                            centroid = poly.representative_point()
                            self.locationlist['bikeshare'].append((centroid.x, centroid.y))
                    else:
                        wkb = wkbfab.create_multipolygon(a)
                        poly = shapely.wkb.loads(wkb, hex=True)
                        centroid = poly.representative_point()
                        self.locationlist['bikeshare'].append((centroid.x, centroid.y))
                
            if ( ('amenity' in a.tags and 
                   a.tags['amenity'] in ['school','kindergarten']) or
                 ('school' in a.tags) ):
                wkb = wkbfab.create_multipolygon(a)
                poly = shapely.wkb.loads(wkb, hex=True)
                centroid = poly.representative_point()
                self.locationlist['schools'].append((centroid.x, centroid.y))
                
            if ( ('amenity' in a.tags and 
                   a.tags['amenity'] in ['hospital','doctors','clinic','pharmacy']) or
                 ('healthcare' in a.tags and 
                   a.tags['healthcare'] in ['alternative','birthing_center','centre','midwife','nurse','hospital','doctor','clinic','pharmacy','yes']) ):
                wkb = wkbfab.create_multipolygon(a)
                poly = shapely.wkb.loads(wkb, hex=True)
                centroid = poly.representative_point()
                self.locationlist['healthcare'].append((centroid.x, centroid.y))
                
            carfree = False
            if 'leisure' in a.tags and a.tags['leisure'] in ['park', 'playground']:
                if not('foot' in a.tags and a.tags['foot'] == 'no'):
                    if not('service' in a.tags and a.tags['service'] == 'private'):
                            if not('access' in a.tags and a.tags['access'] == 'private'):
                                carfree = True
            if 'highway' in a.tags and a.tags['highway'] == 'pedestrian':
                if not('foot' in a.tags and a.tags['foot'] == 'no'):
                    if not('service' in a.tags and a.tags['service'] == 'private'):
                            if not('access' in a.tags and a.tags['access'] == 'private'):
                                carfree = True
            if carfree:
                wkb = wkbfab.create_multipolygon(a)
                poly = shapely.wkb.loads(wkb, hex=True)
                self.carfreelist.append(poly)
                
        except RuntimeError:
            print('RUNTIME ERROR while finding service area')
            
    def way(self, a):
        try:
            carfree = False
            if 'leisure' in a.tags and a.tags['leisure'] in ['park', 'playground']:
                if not('foot' in a.tags and a.tags['foot'] == 'no'):
                    if not('service' in a.tags and a.tags['service'] == 'private'):
                            if not('access' in a.tags and a.tags['access'] == 'private'):
                                carfree = True
            if 'highway' in a.tags and a.tags['highway'] in ['pedestrian', 'path','steps','footway']:
                if not('foot' in a.tags and a.tags['foot'] == 'no'):
                    if 'crossing' not in a.tags and 'sidewalk' not in a.tags:
                        if not('footway' in a.tags and a.tags['footway'] in ['sidewalk','crossing']):
                            if not('service' in a.tags and a.tags['service'] == 'private'):
                                if not('access' in a.tags and a.tags['access'] == 'private'):
                                    carfree = True
            if carfree:
                wkb = wkbfab.create_linestring(a)
                poly = shapely.wkb.loads(wkb, hex=True)
                self.carfreelist.append(poly)
            
        except RuntimeError:
            print('RUNTIME ERROR while finding service way')
        

#haven't used in ages! Need to make sure it returns x,y instead of lat,lon (y,x)
# def get_point_locations(poly, query):
#     #returns a dictionary
    
#     overpass_url = "http://overpass-api.de/api/interpreter" 
#     #check this is good before production
    
#     poly_str = ox.get_polygons_coordinates(poly)[0]
    
#     services = []
    
#     print ('Querying OSM for locations...')
#     data = ox.overpass_request(data={'data':query.format(poly=poly_str)}, timeout=900)
#     for element in data['elements']:
#         if element['type'] == 'node':
#             services.append(
#                     (element['lat'],element['lon'])
#                     )
#         elif 'center' in element:
#             services.append(
#                     (element['center']['lat'],
#                      element['center']['lon'])
#                     )
    
#     return services
    
