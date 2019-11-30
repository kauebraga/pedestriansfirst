# -*- coding: utf-8 -*-
"""isometric.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1xpzFkH7MPwmtFiIfBP2Lg2yqtvzhiF23

# Make sure that the section is running on Python 3
Runtime >> Change Runtime Type >> Runtime Type = Python3

# Setup == Run this every time you use!
"""



# Importação de Pacotes
import networkx as nx # Pacote de redes do Python
import osmnx as ox # Pacote OSM Network X
import geopandas as gpd # Pacote Pandas Georreferenciado
import matplotlib.pyplot as plt # Pacote MatplotLib
from shapely.geometry import Point, LineString, Polygon, mapping # Pacote Shapely (apenas partes do pacote)
from shapely.ops import cascaded_union
from descartes import PolygonPatch #  Pacote Descartes (apenas parte do pacote)
ox.config(log_console=True, use_cache=True)
import utm
import urllib

import ast # Pacote para transformar string em lista
from copy import deepcopy # duplicar memória do Pyhton
from collections import defaultdict # criar dicionário com chave pronta
import fiona # possibilitar exportação
from tqdm import tqdm # Pacote para visualizar downloads
import pandas as pd 

# Pacote Python para plotar gráfico
from pylab import rcParams
rcParams['figure.figsize'] = 16, 25 # Tamanho da figura pode ser customizado

import pdb


# Pacote Python para plotar gráfico
from pylab import rcParams
rcParams['figure.figsize'] = 16, 25 # Tamanho da figura pode ser customizado

"""# Code == Run this too!"""

# Organização do código:
# Main = função principal que chama outras funções

def string_to_list(string):
    """
    Transformação de strings em uma lista
    """
    return ast.literal_eval('[' + string + ']')

def strings_to_list(*args):
    """
    Empacotamento de strings em uma lista
    """
    return list(map(string_to_list, args))

def download_graph(coordinates, distances):
    """
    Criação do grafo de ruas do OSM a partir das coordenadas solicitadas
    """
    max_distance = max(distances)
    
    G = False
    print('Fetching street network')
    for coordinate in tqdm(coordinates, desc='Downloading'):

        if G: # "soma" (merge) com grafo já existente (deepcopy utilizado para não perder grafo entre iterações)
            G = nx.compose(deepcopy(G), ox.graph_from_point(coordinate, 
                                                  distance=max_distance+100,
                                                  network_type='walk'))
        else: # inicializa grafo a partir de todos pontos
            G = ox.graph_from_point(coordinate, distance=max_distance+100, 
                                    network_type='walk')            
    
    return G

def network_buffer(G, distance=100):
    all_gs = gpd.GeoDataFrame(ox.graph_to_gdfs(G,nodes=False,node_geometry=False,fill_edge_geometry=True)).buffer(100)
    new_iso = gpd.GeoSeries(all_gs).unary_union # junção de pontos e edges, mas com buracos nas quadras
    return new_iso

def make_iso_polys(G, center_nodes, distance=500,
                   edge_buff=100, node_buff=0, infill=True):
    """
    Criação do grafo de isótopas
    
    {trip_length: }
    """
    failures = 0
    polygons = []
    l = len(center_nodes)
    
    for i in range (0, l):
        
        center_node = center_nodes[i]
        
        subgraph = nx.ego_graph(G, center_node, radius=distance, 
                                distance='length')
        

        node_points = [Point((data['x'], data['y'])) 
                       for node, data in subgraph.nodes(data=True)]
        
        try:
            nodes_gdf = gpd.GeoDataFrame({'id': subgraph.nodes()},
                                         geometry=node_points)
        
            nodes_gdf = nodes_gdf.set_index('id')

            edge_lines = []
            for n_fr, n_to in subgraph.edges():
                f = nodes_gdf.loc[n_fr]#.geometry
                t = nodes_gdf.loc[n_to]#.geometry
                edge_lines.append(LineString([f,t]))

            n = nodes_gdf.buffer(node_buff).geometry
            e = gpd.GeoSeries(edge_lines).buffer(edge_buff).geometry
            all_gs = list(n) + list(e)
            new_iso = gpd.GeoSeries(all_gs).unary_union # junção de pontos e edges, mas com buracos nas quadras

            # try to fill in surrounded areas so shapes will appear solid and blocks without white space inside them
            if infill:
                new_iso = Polygon(new_iso.exterior)
            polygons.append(new_iso)
        except KeyError:
            print ("FAILURE AT",i)
            failures += 1
        
    isochrone_polys = cascaded_union(polygons) # junta todos os poligonos no entorno das coordenadas 
        
    if failures != 0:
        print(failures,"FAILURES!!!!!!")

    return isochrone_polys, failures


def plot(G, isochrone_polys, center_nodes, distances):
    
    nc = ['blue' if node in center_nodes else 'none' for node in G.nodes()]
    ns = [20 if node in center_nodes else 0 for node in G.nodes()]
    
    iso_colors = ox.get_colors(n=len(distances), cmap='Reds', 
                               start=0.3, return_hex=True)
    
    # plot the network then add isochrones as colored descartes polygon patches
    fig, ax = ox.plot_graph(G, fig_height=8, show=False, close=False, 
                             edge_color='k', edge_alpha=0.2, node_color=nc,
                           node_size=ns)
    
    patch = PolygonPatch(isochrone_polys, fc='red', ec='none', alpha=0.6, zorder=-1)
    ax.add_patch(patch)
    
    plt.show()


def export(polygons, epsg, service, folder=""):
    #returns the result both latlon, projected, but only saves it latlon in geojson
    a = pd.DataFrame(polygons).T
    a = pd.melt(a, value_vars=a.columns)
    a.columns = ['distance', 'polygon']
    a = gpd.GeoDataFrame(a, geometry='polygon')
    a.crs = {'init':'epsg:'+str(epsg)}
    a.geometry = a.geometry.simplify(15)
    b = a.to_crs(epsg=4326)
    #a.to_csv(folder+service+'.csv')
    #a.to_file(folder+service+'.shp')
    #a.to_file(folder+service+'.geojson', driver='GeoJSON')
    b.to_file(folder+service+'latlon'+'.geojson', driver='GeoJSON')
        
    return a, b
    
    print('Polygons saved as',folder+service+'.shp')
    
        
# Main = função principal que recebe coordenadas e distância e chama outras funções
def main(coordinates, distances):
    
    # Convert string to list
    print(coordinates)
    # Download Graph     
    G = download_graph(coordinates, distances)
    
    # Get central nodes
    center_nodes = [ox.get_nearest_node(G, coor) for coor in coordinates]
    
    # Project Graph
    G = ox.project_graph(G)
    
    # Get polygons
    isochrone_polys = make_iso_polys(G, coordinates, distances, center_nodes)
    
    # Plot
    plot(G, isochrone_polys, center_nodes, distances)
    

    # Export
    utm_info = utm.from_latlon(coordinates[0][0],coordinates[0][1])
    if coordinates[0][0]>0: 
        epsg = 32600+utm_info[2]
    elif coordinates[0][1]>0:
        epsg = 32700+utm_info[2]
    else:
        print("invalid UTM")
    a, b = export(isochrone_polys, epsg)
    print ('polygon area:')
    print (a['polygon'].area)
    
    return isochrone_polys, G, a, a[500].area

"""# How to input
coordinates: (-22.94,-43.18), (-22.96,-43.21) :: (lat, long), (lat, long) as a string

distances: 200, 500, 1000 :: distance as string

If you want the isochrone, just convert the trip time and walking speed to distance. Then, copy the result to the isometric calculator.
"""


#@title Time to Distance
#trips_time_in_minutes = "5, 10 , 15" #@param {type:"string"}
#walking_speed = 2.88 #@param {type:"number"}
#speed_unit = "km/h" #@param ["km/h", "m/s"]

#if speed_unit == 'km/h':
#    walking_speed = walking_speed/3.6
    
#list(map(lambda x: walking_speed * x * 60, string_to_list(trips_time_in_minutes)))

#@title Isometric Calculator
#coordinates = [(38.913,-77.026)] #@param {type:"string"}
#distances = [500] #@param {type:"string"}



# setup your projections
#crs_wgs = proj.Proj(init='epsg:4326') # assuming you're using WGS84 geographic
#crs_bng = proj.Proj(init='epsg:27700') # use a locally appropriate projected CRS

# then cast your geographic coordinate pair to the projected system
#x, y = proj.transform(crs_wgs, crs_bng, input_lon, input_lat)



