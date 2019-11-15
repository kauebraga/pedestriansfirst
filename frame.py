import subprocess
import fiona
import os
import json

#import people_near_services


def from_id_hdc(hdc):
    #select city from ID number
    with fiona.open('GHS_STAT_UCDB2015MT_GLOBE_R2019A_V1_0.shp','r') as ucdb:
        for city in ucdb:
            if city['properties']['ID_HDC_G0'] == hdc:
                test_city = city
    #save city geometry so that I can take an extract from planet.pbf within it
    if not os.path.isdir(str(hdc)):
        os.mkdir(str(hdc))
    with open(str(hdc)+'/boundaries.geojson', 'w') as out:
        out.write(json.dumps(test_city))
    #take extract from planet.pbf
    command = "osmium extract albania-latest.pbf -p {}/boundaries.geojson -d {}/ -s simple -v -o city.pbf".format(str(hdc), str(hdc))
    print(command)
    subprocess.call(command.split(' '))
    command = "osmconvert {}/city.pbf >{}/city.pbf".format(str(hdc),str(hdc))
    print(command)
    subprocess.call(command.split(' '))
    command = '''osmfilter {}/city.o5m 
    --drop="area=yes highway=link =motor =proposed 
    =construction =abandoned =platform =raceway 
    service=parking_aisle =driveway =private foot=no" 
    --keep="highway" >{}/city.osm
    '''.format(str(hdc),str(hdc))
    print(command)
    subprocess.call(command.split(' '))
    
    
    #results = people_near_services.pnservices(city, folder_name=str(hdc)+'/')
    #print(str(results))
    
from_id_hdc(3263)