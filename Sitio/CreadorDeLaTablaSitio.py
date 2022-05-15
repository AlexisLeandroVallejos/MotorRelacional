import pandas
import psycopg2

RUTA_CSV = r'.\top-1m.csv'
NOMBRE_DATABASE = "world"
USUARIO = "postgres"
CONTRASENIA = "postgres"
HOST = "localhost"
#con esto guardo en la entidad a blogforha.uol y forha.uol:
REGEX_SEPARADOR = r'[.,]'

#primer punto: obtener countrycode y cctld del BD
def generarDiccionario():
    try:
        query = "SELECT CODE, CODE2 FROM COUNTRY"

        conexion = psycopg2.connect(
            database = NOMBRE_DATABASE,
            user = USUARIO,
            password = CONTRASENIA,
            host = HOST)

        apuntador = conexion.cursor()

        apuntador.execute(query)

        listaCountryCodeCcTLD = apuntador.fetchall()

        dominioDeNivelSuperiorGeográfico = {}

        for (countryCode, ccTLD) in listaCountryCodeCcTLD:
            dominioDeNivelSuperiorGeográfico[ccTLD] = countryCode

    # excepcion por algun error
    except (Exception, psycopg2.Error) as error:
        print("Error al obtener la informacion", error)

    # cerrar apuntador y conexion
    finally:
        if conexion:
            apuntador.close()
            conexion.close()
            print("Conexion con PostgreSQL cerrada")

    return dominioDeNivelSuperiorGeográfico

def creacionDataFrame():
#segundo punto: separa el nro de orden y el dominio
    indiceColumna = ['nroOrden', 'dominio1', 'dominio2', 'dominio3', 'dominio4']
#registros con 5 campos:
#1609,folha.uol.com.br
#24005,blogfolha.uol.com.br
    millonDf = pandas.read_csv(RUTA_CSV,
                           header = None,
                           sep = REGEX_SEPARADOR,
                           names = indiceColumna)
    return millonDf

def creacionDeLaTabla():
    try:
        query = "CREATE TABLE IF NOT EXISTS SITIO(" \
                "ID INT NOT NULL PRIMARY KEY," \
                "ENTIDAD VARCHAR," \
                "PAIS VARCHAR," \
                "EXTRA VARCHAR," \
                "COUNTRYCODE CHAR(3) FOREIGN KEY REFERENCES COUNTRY.CODE);"

        conexion = psycopg2.connect(
            database = NOMBRE_DATABASE,
            user = USUARIO,
            password = CONTRASENIA,
            host = HOST)

        apuntador = conexion.cursor()

        apuntador.execute(query)

        listaCountryCodeCcTLD = apuntador.fetchall()

        dominioDeNivelSuperiorGeográfico = {}

        for (countryCode, ccTLD) in listaCountryCodeCcTLD:
            dominioDeNivelSuperiorGeográfico[ccTLD] = countryCode

    # excepcion por algun error
    except (Exception, psycopg2.Error) as error:
        print("Error al obtener la informacion", error)

    # cerrar apuntador y conexion
    finally:
        if conexion:
            apuntador.close()
            conexion.close()
            print("Conexion con PostgreSQL cerrada")

    return dominioDeNivelSuperiorGeográfico