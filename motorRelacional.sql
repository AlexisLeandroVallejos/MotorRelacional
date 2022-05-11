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

----------Simples3.2.1----------
SELECT POPULATION
FROM COUNTRY 
WHERE NAME = 'Argentina';

----------Simples3.2.3----------
SELECT NAME
FROM COUNTRY
WHERE CONTINENT = 'South America' 
	AND POPULATION > 15000000;
	
----------Simples3.2.7----------
SELECT 
	CONTINENT, 
	COUNT(NAME) AS CANTIDAD_DE_PAISES 
FROM COUNTRY
WHERE POPULATION > 20000000
GROUP BY CONTINENT
HAVING COUNT(NAME) > 15;

----------Subqueries3.2.2----------
SELECT NAME, LIFEEXPECTANCY
FROM COUNTRY
WHERE LIFEEXPECTANCY = (SELECT MIN(LIFEEXPECTANCY) FROM COUNTRY) 
OR LIFEEXPECTANCY = (SELECT MAX(LIFEEXPECTANCY) FROM COUNTRY);

----------Subqueries3.2.4----------
SELECT CONTINENT
FROM COUNTRY
WHERE
(SELECT SUM(GNP) AS TOTAL_GNP 
 FROM COUNTRY 
 GROUP BY CONTINENT 
 ORDER BY TOTAL_GNP DESC LIMIT 3)

--SELECT AVG(GNP) AS GNP_PROMEDIO FROM COUNTRY;
--SELECT SUM(GNP) AS TOTAL_GNP FROM COUNTRY;