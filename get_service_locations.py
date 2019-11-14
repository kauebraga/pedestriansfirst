import requests
import json
import fiona
import osmnx

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


def get_point_locations(poly, query):
    #returns a dictionary
    
    overpass_url = "http://overpass-api.de/api/interpreter" 
    #check this is good before production
    
    poly_str = osmnx.get_polygons_coordinates(poly)[0]
    
    services = []
    
    print ('Querying OSM for locations...')
    data = osmnx.overpass_request(data={'data':query.format(poly=poly_str)}, timeout=900)
    for element in data['elements']:
        if element['type'] == 'node':
            services.append(
                    (element['lat'],element['lon'])
                    )
        elif 'center' in element:
            services.append(
                    (element['center']['lat'],
                     element['center']['lon'])
                    )
    
    return services