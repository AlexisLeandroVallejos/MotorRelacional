import geopandas
import numpy as np
import matplotlib.pyplot as plt
import psycopg2
from geopandas import GeoSeries, GeoDataFrame

NOMBRE_DATABASE = "world"
USUARIO = "postgres"
CONTRASENIA = "postgres"
HOST = "localhost"

conexion = psycopg2.connect(
    database = NOMBRE_DATABASE,
    user = USUARIO,
    password = CONTRASENIA,
    host = HOST)

#query = "SELECT NAME, POPULATION FROM COUNTRY"
#geom_col = 'NAME'
#df = GeoDataFrame.from_postgis(query, conexion, geom_col = 'name')

world = GeoDataFrame.from_file('ne_10m_admin_0_countries.shp')
#world['name'] = range(len(world))
#print(world.columns)
if '__main__':
    print(world)



# para ver los colormap, ejecutar colors.py
#world.plot(column='prueba', colormap='Greens', alpha=0.5, categorical=False, legend=False, axes=None)
#world.plot(column='prueba', colormap='binary', alpha=0.5, categorical=False, legend=False, axes=None)
#world.plot()
#world.plot(column=None, colormap='Greens', alpha=0.5, categorical=False, legend=False, axes=None)

#America del Sur

#print(world['CONTINENT'].unique())

#south = world[world['CONTINENT'] == 'South America']
#south.plot(column='prueba', colormap='binary', alpha=0.5, categorical=False, legend=False, axes=None)

#world.plot(column='prueba', cmap='Greens', alpha=0.5, categorical=False, legend=False, ax=None)

#plt.show()

