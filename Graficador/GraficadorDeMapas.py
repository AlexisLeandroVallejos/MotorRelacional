import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import psycopg2

RUTA_ARCHIVO = r'.\ne_10m_admin_0_countries.shp'
NOMBRE_DATABASE = "world"
USUARIO = "postgres"
CONTRASENIA = "postgres"
HOST = "localhost"

#queries:
queryPoblacionMundial = """
SELECT 
    CODE, POPULATION
FROM 
    COUNTRY
"""

queryProductoBrutoMundial = """
SELECT 
    CODE, GNP
FROM 
    COUNTRY
"""

queryCantSitiosMundial = """
SELECT 
    COUNTRYCODE, COUNT(PAIS) AS CANTIDADSITIOS
FROM 
    SITIO
GROUP BY
    COUNTRYCODE
"""

queryCantSitiosMundial2 = """
SELECT 
    COUNTRYCODE, COUNT(PAIS) AS CANTIDADSITIOS
FROM 
    SITIO2
GROUP BY
    COUNTRYCODE
"""

#leer archivo geografico:
#(renombrando columna para que el merge funcione despues)
gpdworld = gpd.read_file(RUTA_ARCHIVO).rename(columns = {'ADM0_A3': 'code'})
#world = GeoDataFrame.from_file(RUTA_ARCHIVO)

#try, exception y close para interactuar con postgres
try:
    #conexion a la BD
    conexion = psycopg2.connect(
            database = NOMBRE_DATABASE,
            user = USUARIO,
            password = CONTRASENIA,
            host = HOST)

    #traer de la BD las consultas como dataframes
    dfCountryCodeYPoblacion = pd.read_sql(queryPoblacionMundial, conexion)
    dfCountryCodeYGnp = pd.read_sql(queryProductoBrutoMundial, conexion)
        # sitio.countrycode debe matchear con gdpworld.code:
    dfCountryCodeYCantSitios = pd.read_sql(queryCantSitiosMundial, conexion).rename(columns={'countrycode': 'code'})
    #dfCountryCodeYCantSitios2 = pd.read_sql(queryCantSitiosMundial2, conexion).rename(columns = {'countrycode': 'code'})


# excepcion por algun error
except (Exception, psycopg2.Error) as error:
    print("Error al obtener la informacion", error)

# cerrar conexion
finally:
    if conexion:
        conexion.close()
        print("Conexion con PostgreSQL cerrada")

    #Unir a un dataframe nuevo, uniendo dataframes gdpworld y consultas
    dataFrameDePoblacionMundial = pd.merge(
        gpdworld, dfCountryCodeYPoblacion,
        on = 'code', how = 'inner'
    )

    dataFrameDeGnpMundial = pd.merge(
        gpdworld, dfCountryCodeYGnp,
        on = 'code', how = 'inner'
    )

    dataFrameDeSitiosMundial = pd.merge(
        gpdworld, dfCountryCodeYCantSitios,
        on='code', how='inner'
    )

    #dataFrameDeSitiosMundial2 = pd.merge(gpdworld, dfCountryCodeYCantSitios2,on='code', how='inner')

if '__main__':
    #mapas:
    poblacionMundial = dataFrameDePoblacionMundial.plot(
        column='population',
        cmap = 'nipy_spectral',
        alpha=0.5,
        categorical=False,
        legend=True,
        ax=None)

    pbiMundial = dataFrameDeGnpMundial.plot(
        column='gnp',
        cmap='nipy_spectral',
        alpha=0.5,
        categorical=False,
        legend=True,
        ax=None)

    sitiosMundial = dataFrameDeSitiosMundial.plot(
        column='cantidadsitios',
        cmap='nipy_spectral',
        alpha=0.5,
        categorical=False,
        legend=True,
        ax=None)

    """
    sitiosMundial2 = dataFrameDeSitiosMundial2.plot(
        column='cantidadsitios',
        cmap='nipy_spectral',
        alpha=0.5,
        categorical=False,
        legend=True,
        ax=None)
"""

    #titulo
    poblacionMundial.set_title("Poblacion Mundial")
    pbiMundial.set_title("PBI Mundial")
    sitiosMundial.set_title("Cantidad de sitios por pais")
    #sitiosMundial2.set_title("Cantidad de sitios por pais")

    #mostrar todas
    plt.show()