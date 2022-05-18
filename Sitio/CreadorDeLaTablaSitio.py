import csv
import os
import pandas as pd
import psycopg2

CSV_PATH = r'.\top-1m.csv'
NOMBRE_DATABASE = "world"
USUARIO = "postgres"
CONTRASENIA = "postgres"
HOST = "localhost"

queryCountryCodeYCctld = "SELECT CODE, CODE2 FROM COUNTRY"

queryDropTabla = """
    DROP TABLE IF EXISTS sitio2;
"""

queryCrearTabla = """
    CREATE TABLE IF NOT EXISTS sitio2(
        id int NOT NULL,
        entidad varchar,
        subentidad varchar,
        subentidad2 varchar,
        tipo_entidad varchar,
        pais varchar,
        countrycode char(3),
        PRIMARY KEY (id),
        FOREIGN KEY (countrycode) REFERENCES COUNTRY(CODE)
    );
"""

#filtros, segun cantidad de campos:
def tieneCuatroCampos(country, entity, index, item, splitedDomain, subentity, subentity2, type):
    if len(splitedDomain) == 4:
        if len(splitedDomain[3]) == 2:
            if (index == 0):
                entity = item
            elif (index == 1):
                subentity = item
            elif (index == 2):
                type = item
            elif (index == 3):
                country = item
        else:
            if (index == 0):
                entity = item
            elif (index == 1):
                subentity = item
            elif (index == 2):
                subentity2 = item
            elif (index == 3):
                type = item
    return country, entity, subentity, subentity2, type


def tieneTresCampos(country, entity, index, item, splitedDomain, subentity, type):
    if len(splitedDomain) == 3:
        if len(splitedDomain[2]) == 2:
            if (index == 0):
                entity = item
            elif (index == 1):
                type = item
            elif (index == 2):
                country = item
        else:
            if (index == 0):
                entity = item
            elif (index == 1):
                subentity = item
            elif (index == 2):
                type = item
    return country, entity, subentity, type


def tieneDosCampos(country, entity, index, item, splitedDomain, type):
    if len(splitedDomain) == 2:
        if len(splitedDomain[1]) == 2:
            if (index == 0):
                entity = item
            elif (index == 1):
                country = item
        else:
            if (index == 0):
                entity = item
            elif (index == 1):
                type = item
    return country, entity, type

def getSiteData(siteRow):
    # Se asignan los datos de cada registro
    id, entity, subentity, subentity2, type, country = "", "", "", "", "", ""
    splitedDomain = siteRow[1].split('.')
    id = siteRow[0]

    for index, item in enumerate(splitedDomain):
        country, entity, type = tieneDosCampos(country, entity, index, item, splitedDomain, type)
        country, entity, subentity, type = tieneTresCampos(country, entity, index, item, splitedDomain, subentity, type)
        country, entity, subentity, subentity2, type = tieneCuatroCampos(country, entity, index, item, splitedDomain, subentity, subentity2, type)
    return (id, entity, subentity, subentity2, type, country)

with open(CSV_PATH) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    data = []
    for siteRow in csv_reader:
        data.append(getSiteData(siteRow))

try:
    #conectar, usar apuntador para queries
    dominioDeNivelSuperiorGeográfico = {}

    columnasSinCountryCode = ['id',
                              'entidad',
                              'subentidad',
                              'subentidad2',
                              'tipoEntidad',
                              'pais']

    #conexion
    conexion = psycopg2.connect(
        database = NOMBRE_DATABASE,
        user = USUARIO,
        password = CONTRASENIA,
        host = HOST)

    #cursor
    apuntador = conexion.cursor()

    # Traer la consulta queryCountryCodeYCctld
    apuntador.execute(queryCountryCodeYCctld)

    #listaDeTuplas del BD
    listaCountryCodeCcTLD = apuntador.fetchall()

    #lower para poder reemplazar despues
    #...
    for (countryCode, ccTLD) in listaCountryCodeCcTLD:
        dominioDeNivelSuperiorGeográfico[ccTLD.lower()] = countryCode.lower()

    #----dataframe del csv filtrado----
    millonSinCountryCode = pd.DataFrame(data, columns = columnasSinCountryCode)

    #dataframe de la consulta countrycode, pais
    columnasCountryCodePais = ['countrycode', 'pais']
    dfCountryCodePais = pd.DataFrame(listaCountryCodeCcTLD, columns = columnasCountryCodePais)

    #reemplazar por minusculas para matchear columna del merge
    dfCountryCodePais['pais'] = dfCountryCodePais['pais'].str.lower()

    #reemplazar valores faltantes de pais para hacer el match con merge
    millonSinCountryCode['pais'] = millonSinCountryCode['pais'].replace(to_replace ='', value ='us')

    #merge
    millonConCountryCode = pd.merge(
        millonSinCountryCode, dfCountryCodePais,
        on = 'pais', how = 'inner')
    #millonConCountryCode.subentidad2.value_counts()

    #drop tabla si existe
    apuntador.execute(queryDropTabla)

    #crear tabla
    apuntador.execute(queryCrearTabla)

    #mandar el dataframe a csv
    PATH_DFMILLONCSV = './dataframe_millon.csv'
    millonConCountryCode.to_csv(PATH_DFMILLONCSV, index = False, header = False)

    #leer archivo
    archivo = open(PATH_DFMILLONCSV, 'r')

    #copiar al postgresql
    apuntador.copy_from(archivo, 'sitio2', sep = ',')
    conexion.commit()

# excepcion por algun error
except (Exception, psycopg2.Error) as error:
    os.remove(PATH_DFMILLONCSV)
    print("Error al obtener la informacion", error)
    conexion.rollback()
    apuntador.close()

# cerrar apuntador y conexion
finally:
    if conexion:
        apuntador.close()
        conexion.close()
        print("Conexion con PostgreSQL cerrada")