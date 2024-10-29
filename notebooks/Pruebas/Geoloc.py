# Databricks notebook source
from geopy.geocoders import Nominatim

# Initialize Nominatim API
geolocator = Nominatim(user_agent="MyApp")

location = geolocator.geocode("Hyderabad")

print("The latitude of the location is: ", location.latitude)
print("The longitude of the location is: ", location.longitude)

coordinates = "40.4172 , -3.7018341"

location = geolocator.reverse(coordinates)

address = location.raw['address']

# Traverse the data
city = address.get('city', '')
state = address.get('state', '')
country = address.get('country', '')
neighbourhood=address.get('neighbourhood','')
neighbourhood=address.get('village','')
print(city,state,country,neighbourhood)