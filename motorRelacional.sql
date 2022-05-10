/* 
Comandos iniciales, Solo hacer la primera vez al crear

En cmd/terminal/bash:
Para crear la base de datos world:
----------3.1.1----------
createdb -U postgres world

Para agregar el script world.sql a la db:
----------3.1.2----------
psql -f "rutaArchivo" -U postgres -d world

*postgres como usuario default/super del postgresql
*/

----------3.1.3----------
--SELECT * FROM CITY;
SELECT * FROM COUNTRY;
--SELECT * FROM COUNTRYLANGUAGE

----------3.2.1----------
SELECT POPULATION
FROM COUNTRY 
WHERE NAME = 'Argentina';

----------3.2.3----------
SELECT NAME
FROM COUNTRY
WHERE CONTINENT = 'South America' 
	AND POPULATION > 15000000;
	
----------3.2.7----------
SELECT 
	CONTINENT, 
	COUNT(NAME) AS CANTIDAD_DE_PAISES 
FROM COUNTRY
WHERE POPULATION > 20000000
GROUP BY CONTINENT
HAVING COUNT(NAME) > 15;




