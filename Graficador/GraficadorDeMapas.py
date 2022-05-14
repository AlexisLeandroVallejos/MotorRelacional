import numpy as np
import matplotlib.pyplot as plt
import psycopg2
import geopandas

RUTA_ARCHIVO = r'F:\PythonProjects\MotorRelacional\Graficador\ne_10m_admin_0_countries.shp'
NOMBRE_DATABASE = "world"
USUARIO = "postgres"
CONTRASENIA = "postgres"
HOST = "localhost"


query = "SELECT name, population FROM country"

conexion = psycopg2.connect(
    database = NOMBRE_DATABASE,
    user = USUARIO,
    password = CONTRASENIA,
    host = HOST)

#world = GeoDataFrame.from_file(RUTA_ARCHIVO)
world = geopandas.read_file(RUTA_ARCHIVO)
world['prueba'] = range(len(world))


if '__main__':
    world.columns
    poblacionMundial = world.plot(column='POP_EST',
                                  cmap = 'nipy_spectral',
                                  alpha=0.5,
                                  categorical=False,
                                  legend=True,
                                  axes=None)
    pbiMundial = world.plot(column='GDP_MD_EST',
                            cmap='nipy_spectral',
                            alpha=0.5,
                            categorical=False,
                            legend=True,
                            axes=None)
    poblacionMundial.set_title("Poblacion Mundial")
    pbiMundial.set_title("PBI Mundial")

    plt.show()
    # para ver los colormap, ejecutar colors.py
    #world.plot(column='prueba', colormap='Greens', alpha=0.5, categorical=False, legend=False, axes=None)
    #world.plot(column='prueba', colormap='binary', alpha=0.5, categorical=False, legend=False, axes=None)
    #world.plot()
    #world.plot(column=None, colormap='Greens', alpha=0.5, categorical=False, legend=False, axes=None)

    # America del Sur

    # print(world['CONTINENT'].unique())

    # south = world[world['CONTINENT'] == 'South America']
    # south.plot(column='prueba', colormap='binary', alpha=0.5, categorical=False, legend=False, axes=None)

    # world.plot(column='prueba', cmap='Greens', alpha=0.5, categorical=False, legend=False, ax=None)



#geom_col = 'name'
#df = GeoDataFrame.from_postgis(query, conexion)
#df = geopandas.read_postgis(query, conexion)



