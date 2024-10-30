
from geopy.geocoders import Nominatim

#No hemos encontrado ninguna otra forma de tener un registro que relacione los códigos postales con los distritos, así que lo hacemos manualmente. Ligera chapuza.
dict_dist={'28001':4,
'28006':4,
'28028':4,
'28013':1,
'28012':1,
'28024':1,
'28005':2,
'28045':2,
'28009':3,
'28007':3,
'28014':3,
'28035':8,
'28034':8,
'28029':8,
'28050':8,
'28049':8,
'28048':8,
'28002':5,
'28016':5,
'28036':5,
'28039':6,
'28020':6,
'28003':7,
'28015':7,
'28010':7,
'28023':9,
'28040':9,
'28008':9,
'28011':9,
'28024':10,
'28047':10,
'28044':10,
'28014':11,
'28025':11,
'28026':12,
'28041':12,
'28053':13,
'28018':13,
'28038':13,
'28030':14,
'28017':15,
'28027':15,
'28055':16,
'28033':16,
'28043':16,
'28021':17,
'28051':18,
'28032':19,
'28052':19,
'28022':20,
'28037':20,
'28042':21,
}
# Iniciamos el servicio
def location_to_zipcode(coordinates):
    geolocator = Nominatim(user_agent="MyApp")
    location = geolocator.reverse(coordinates)
    address=location.raw['address']
    if 'postcode' in address:
        return address['postcode']
    else:
        return None

def zipcode_to_district(zipcode):
    if zipcode in dict_dist:
        return dict['zipcode']
    else: 
        return None

def location_to_district(location):
    return zipcode_to_district(location_to_zipcode(location))


