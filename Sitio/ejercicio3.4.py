import psycopg2
import csv

def getSiteData(siteRow):
    # Se asignan los datos de cada registro
    id, entity, type, country, countryCode = "", "", "", "", ""
    splitedDomain = siteRow[1].split('.')
    id = siteRow[0]

    for index, item in enumerate(splitedDomain):
        if (index == 0):
            entity = item
        elif (len(item) == 3):
            type = item
        elif (len(item) == 2):
            country = item

    if country.upper() in code_dict:
        countryCode = code_dict[country.upper()]
    else:
        countryCode = "USA"
    
    return (id, entity, type, country, countryCode)

# Se crea la conexion a la database
connection = psycopg2.connect(database="world", user="postgres", password="123456")
#connection = psycopg2.connect(database="world", user="postgres", password="postgres")
cursor = connection.cursor()

# Se crea la tabla sitio
cursor.execute("DROP TABLE IF EXISTS SITIO")
cursor.execute(
    "CREATE TABLE IF NOT EXISTS SITIO(" \
    "ID INT NOT NULL PRIMARY KEY," \
    "ENTIDAD VARCHAR," \
    "TIPO_ENTIDAD VARCHAR," \
    "PAIS VARCHAR," \
    "COUNTRYCODE VARCHAR(3) REFERENCES COUNTRY(CODE));"
)

# Se crea el diccionario de codigos de pais
query = "SELECT CODE, CODE2 FROM COUNTRY"
cursor.execute(query)
code_list = cursor.fetchall()
connection.commit()
code_dict = {}
for code, code2 in code_list:
    code_dict[code2] = code

# Se lee el archivo csv
CSV_PATH = "top-1m.csv"
query = "INSERT INTO SITIO(ID, ENTIDAD, TIPO_ENTIDAD, PAIS, COUNTRYCODE) VALUES "
with open(CSV_PATH) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter = ',')  
    data = []                                            
    for siteRow in csv_reader:
        data.append(getSiteData(siteRow))

# Se insertan los datos en la base
args = ','.join(cursor.mogrify("(%s, %s, %s, %s, %s)", i).decode('utf-8') for i in data)
cursor.execute(query + (args))
connection.commit()

connection.close()