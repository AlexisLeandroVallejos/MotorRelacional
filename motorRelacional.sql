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

-- 3.2.2 simple
SELECT DISTINCT continent 
FROM country;

----------Simples3.2.3----------
SELECT NAME
FROM COUNTRY
WHERE CONTINENT = 'South America' 
	AND POPULATION > 15000000;
	
-- 3.2.4 simple
SELECT NAME, GNP
FROM COUNTRY
ORDER BY GNP DESC LIMIT 10;

-- 3.2.5 simple
SELECT GOVERNMENTFORM, COUNT(NAME) AS CANTIDAD
FROM COUNTRY
GROUP BY GOVERNMENTFORM
ORDER BY CANTIDAD DESC;

-- 3.2.6 simple
SELECT CONTINENT, SUM(SURFACEAREA) AS SURFACE
FROM COUNTRY
GROUP BY CONTINENT
ORDER BY SURFACE DESC;
	
----------Simples3.2.7----------
SELECT 
	CONTINENT, 
	COUNT(NAME) AS CANTIDAD_DE_PAISES 
FROM COUNTRY
WHERE POPULATION > 20000000
GROUP BY CONTINENT
HAVING COUNT(NAME) > 15;

-- 3.2.1 subquery
-- Devuelve el pais con menor expectativa de vida
SELECT NAME, LIFEEXPECTANCY
FROM COUNTRY
WHERE LIFEEXPECTANCY = (
	SELECT MIN(LIFEEXPECTANCY) 
	FROM COUNTRY);

----------Subqueries3.2.2----------
SELECT NAME, LIFEEXPECTANCY
FROM COUNTRY
WHERE LIFEEXPECTANCY = (SELECT MIN(LIFEEXPECTANCY) FROM COUNTRY) 
OR LIFEEXPECTANCY = (SELECT MAX(LIFEEXPECTANCY) FROM COUNTRY);

-- 3.2.3 subquery
SELECT NAME, INDEPYEAR
FROM COUNTRY
WHERE CONTINENT = (
	SELECT CONTINENT 
	FROM COUNTRY 
	WHERE INDEPYEAR = (
		SELECT MIN(INDEPYEAR) 
		FROM COUNTRY));

----------Subqueries3.2.4----------
SELECT 
	CONTINENT 
FROM 
	(SELECT 
	 	CONTINENT, SUM(GNP) AS TOTAL_GNP 
	 FROM 
	 	COUNTRY 
	 GROUP BY 
	 	CONTINENT 
	 HAVING 
	 	SUM(GNP) > (SELECT SUM(GNP)/7 FROM COUNTRY) 
	 ORDER BY 
	 	TOTAL_GNP DESC)
AS CONTINENTES_POR_GNP;

-- 3.2.1 joins
SELECT CO.NAME, CL.LANGUAGE
FROM COUNTRY AS CO, COUNTRYLANGUAGE AS CL
WHERE CO.CODE = CL.COUNTRYCODE 
AND CO.CONTINENT = 'Oceania';

-- 3.2.2 joins
SELECT CO.NAME, COUNT(CL.COUNTRYCODE) AS LANGUAGES
FROM COUNTRY AS CO, COUNTRYLANGUAGE AS CL
WHERE CO.CODE = CL.COUNTRYCODE
GROUP BY CO.NAME
HAVING COUNT(CL.COUNTRYCODE) > 1
ORDER BY LANGUAGES DESC;

----------Joins3.2.3----------
SELECT 
	LANGUAGE
FROM
	COUNTRY
LEFT JOIN 
	COUNTRYLANGUAGE
ON 
	CODE = COUNTRYCODE
WHERE 
	CONTINENT = (SELECT 
				 	CONTINENT 
				 FROM 
				 	(SELECT 
					 	CONTINENT, SUM(GNP) AS TOTAL_GNP 
					 FROM 
					 	COUNTRY 
					 WHERE 
					 	CONTINENT <> 'Antarctica' 
					 GROUP BY 
					 	CONTINENT 
					 ORDER BY 
					 	TOTAL_GNP ASC 
					 LIMIT 1) 
				 AS GNP_CONTINENTE_MAS_POBRE);

----------Joins3.2.4----------
---1---
SELECT NAME, POPULATION
FROM COUNTRY
ORDER BY NAME;

---2---
-- 3.2.4 joins
SELECT CO.NAME, CO.POPULATION, SUM(CI.POPULATION) AS URBAN_POPULATION, SUM(CI.POPULATION) * 100 / CO.POPULATION AS URBAN_PERCENT
FROM COUNTRY AS CO, CITY AS CI
WHERE CO.CODE = CI.COUNTRYCODE 
GROUP BY CO.CODE
ORDER BY URBAN_PERCENT DESC;
	
----------3.3----------
--1--
CREATE TABLE IF NOT EXISTS STATS(
	COUNTRYCODE CHAR(3) PRIMARY KEY NOT NULL,
	CANT_LENGUAS TEXT,
	POP_URBANA BIGINT,
	FOREIGN KEY (COUNTRYCODE)
		REFERENCES COUNTRY(CODE)
);

--2--
--FORMA CORTA, Execution Time: 13.148 ms
INSERT INTO 
	STATS (COUNTRYCODE, CANT_LENGUAS, POP_URBANA)
	SELECT
		SUBQUERY.COUNTRYCODE, 
		CANT_LENGUAS, 
		SUM(CI.POPULATION) AS POP_URBANA
	FROM (SELECT
		  	CO.CODE AS COUNTRYCODE,
		  	COUNT(CL.LANGUAGE) AS CANT_LENGUAS 
		  FROM
		  	COUNTRY AS CO
		  INNER JOIN 
		  	COUNTRYLANGUAGE AS CL
		  ON 
		  	CO.CODE = CL.COUNTRYCODE  
		  GROUP BY
		  	CO.CODE) 
	AS SUBQUERY
	LEFT JOIN
		CITY AS CI
	ON
		SUBQUERY.COUNTRYCODE = CI.COUNTRYCODE
	GROUP BY
		SUBQUERY.COUNTRYCODE, SUBQUERY.CANT_LENGUAS;
		
/* 
FORMA LARGA, Execution Time: 15,802 ms
--2--
INSERT INTO
	STATS
	SELECT
		COUNTRY.CODE, COUNT(COUNTRYLANGUAGE.LANGUAGE) AS CANTLANG 
	FROM
		COUNTRY
	INNER JOIN 
		COUNTRYLANGUAGE 
	ON 
		COUNTRYLANGUAGE.COUNTRYCODE = COUNTRY.CODE 
	GROUP BY
		COUNTRY.CODE;

--3--
UPDATE 
	STATS
SET 
	COUNTRYCODE = SUBQUERY.CODE,
	POP_URBANA = SUBQUERY.POP_URBANA
FROM
	(SELECT
		COUNTRY.CODE, SUM(CITY.POPULATION) AS POP_URBANA 
	FROM
		COUNTRY
	INNER JOIN 
		CITY 
	ON 
		COUNTRY.CODE = CITY.COUNTRYCODE 
	GROUP BY
		COUNTRY.CODE) 
AS 
	SUBQUERY
WHERE 
	STATS.COUNTRYCODE = SUBQUERY.CODE;
*/

----------EJERCICIO4----------
SELECT * FROM sitio;

/*
SELECT * FROM sitio2
ORDER BY ID;
*/
----------EJERCICIO5----------
--1--
select *
from sitio s1, sitio s2
where s1.countrycode = s2.countrycode
and s1.entidad like 'a%' and s2.entidad like 'b%'
limit 100;

/*
La consulta trae todas las columnas de sitio, donde sitio(s1) estara acoplado a una copia(s2) con condicion:
- countrycode de s1 sera igual a countrycode de s2
- entidad de s1 empezara con la letra 'a'
- entidad de s2 empezara con la letra 'b'
- limitar la consulta a las primeras 100 filas
*/

--2--
explain analyze select *
from sitio s1 ,sitio s2
where s1.countrycode = s2.countrycode
and s1.entidad like 'a%' and s2.entidad like 'b%'
limit 100;

--3--
DROP INDEX COUNTRYCODE;
CREATE INDEX COUNTRYCODE ON SITIO(COUNTRYCODE);

--4--
/*
Segun las imagenes, se puede ver como los datos son organizados para lograr la consulta, 
cada procedimiento necesario para realizarla. 
Cuando se compara el explain previo a la creacion del indice con el posterior,
se observa una reduccion del costo de la consulta. El indice permite una organizacion de datos mas rapida
y por ende una ejecucion mas rapida de la consulta.
*/