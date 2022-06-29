import csv
import psycopg2

CSV_PATH = r'.\top-1m.csv'
NOMBRE_DATABASE = "world"
USUARIO = "postgres"
CONTRASENIA = "postgres"
HOST = "localhost"
TOTAL = 1000000
PROCESADOS = 1
CODIGO_EEUU = "USA"

#queries:
DROPEAR_TABLA_SITIO_V2 = "DROP TABLE IF EXISTS SITIO_V2"

CREAR_TABLA_SITIO_V2 = """
CREATE TABLE IF NOT EXISTS SITIO_V2(
    ID INT NOT NULL PRIMARY KEY,
    ENTIDAD VARCHAR,
    TIPO_ENTIDAD VARCHAR,
    PAIS VARCHAR,
    COUNTRYCODE VARCHAR(3) REFERENCES COUNTRY(CODE));
"""

SELECT_CODIGOPAIS_PAIS = "SELECT CODE, CODE2 FROM COUNTRY"

INSERTAR_FILA = """INSERT INTO SITIO_V2(ID, ENTIDAD, TIPO_ENTIDAD, PAIS, COUNTRYCODE) VALUES (%s, %s, %s, %s, %s);"""

#diccionarios para subir al postgres
diccSitios = {}
diccSitiosError = {}

#condicionales para armar valores
def tieneDosCampos(camposDelSitio):
    return len(camposDelSitio) == 2

def tieneTresCampos(camposDelSitio):
    return len(camposDelSitio) == 3

def tieneCuatroCampos(camposDelSitio):
    return len(camposDelSitio) == 4

#transforma los dominios de 4 campos a 3
def transformarEnTresCampos(camposDelSitio):
    return camposDelSitio[0] + "." + camposDelSitio[1] + "-DOT-" + camposDelSitio[2] + "." + camposDelSitio[3]

#averiguar countrycode
def averiguarCountryCode(pais):
    if pais.upper() in code_dict:
        return code_dict[pais.upper()]
    else:
        return CODIGO_EEUU

#crear los valores del diccionario:
def armadorTuplaDoble(numero, camposDelSitio):
    id = numero
    entidad = camposDelSitio[0]
    tipo_entidad = None
    pais = camposDelSitio[1]
    countryCode = averiguarCountryCode(camposDelSitio[1])
    return (id, entidad, tipo_entidad, pais, countryCode)

def armadorTuplaTriple(numero, camposDelSitio):
    id = numero
    entidad = camposDelSitio[0]
    tipo_entidad = camposDelSitio[1]
    pais = camposDelSitio[2]
    countryCode = averiguarCountryCode(camposDelSitio[2])
    return (id, entidad, tipo_entidad, pais, countryCode)

def armadorTuplaCuadruple(numero, camposDelSitio):
    nuevosCampos = transformarEnTresCampos(camposDelSitio).split(".")
    return armadorTuplaTriple(numero, nuevosCampos)

#guardados/total
def calcularPorcentaje():
    return (PROCESADOS / TOTAL)

#guarda elementos del csv en diccionario
def guardarEnDiccSitios(filaDeSitio):
    numero = filaDeSitio[0]
    sitio = filaDeSitio[1]
    camposDelSitio = sitio.split(".")
    if tieneDosCampos(camposDelSitio):
        diccSitios[numero] = armadorTuplaDoble(numero, camposDelSitio)
    elif tieneTresCampos(camposDelSitio):
        diccSitios[numero] = armadorTuplaTriple(numero, camposDelSitio)
    elif tieneCuatroCampos(camposDelSitio):
        diccSitios[numero] = armadorTuplaCuadruple(numero, camposDelSitio)
    else:
        diccSitiosError[numero] = sitio

# Se crea la conexion a la database
#connection = psycopg2.connect(database="world", user="postgres", password="123456")
connection = psycopg2.connect(database="world", user="postgres", password="postgres")
cursor = connection.cursor()

# Se crea la tabla sitio_v2
cursor.execute(DROPEAR_TABLA_SITIO_V2)
cursor.execute(CREAR_TABLA_SITIO_V2)

# Se crea el diccionario de codigos de pais
cursor.execute(SELECT_CODIGOPAIS_PAIS)
code_list = cursor.fetchall()
connection.commit()
code_dict = {}
for code, code2 in code_list:
    code_dict[code2] = code

#guardar elementos del csv al diccionario
with open(CSV_PATH) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    for siteRow in csv_reader:
        guardarEnDiccSitios(siteRow)
        print("Elementos del CSV subidos al diccionario: {} de {} | {:.2%}".format(PROCESADOS, TOTAL, calcularPorcentaje()))
        PROCESADOS += 1
    print("Todos los elementos del CSV estan en el diccionario.")

#usar mogrify para pasar la consulta a postgresql y decode
def insertarFila(tuplaDeCampos):
    return cursor.mogrify(INSERTAR_FILA, tuplaDeCampos).decode('utf-8')

#inserta sitios a la tabla sitio_v2 desde el diccionario
PROCESADOS = 1
for tuplaDeCampos in diccSitios.values():
    insertarUnSitio = insertarFila(tuplaDeCampos)
    cursor.execute(insertarUnSitio)
    print("Sitios subidos a Postgresql: {} de {} | {:.2%}".format(PROCESADOS, TOTAL, calcularPorcentaje()))
    PROCESADOS += 1
connection.commit()
connection.close()
print("Todos los elementos del diccionario estan en la tabla sitio_v2 en postgresql.")
#si el pais es .uk no estara en el diccionario code, code2 y sera countrycode USA (debe ser .gb para que sea GBR)
