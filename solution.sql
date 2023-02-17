-- 1) Quien es el piloto con mas vuelos A
-- 
-- Para conseguir el piloto con mas vuelos A se filtran los vuelos A
-- luego se agrupa por piloto, seguido se ordena de forma descendiente por la columna count
-- y finalmente se limita al primer registro generando entonces el piloto con mas vuelos A

SELECT Piloto, count(*) from vv_vuelos where OnTime = 'A' group by Piloto order by 2 desc limit 1;

-- 2) Cual es la aerolinea con mas vuelos C
-- 
-- Para conseguir la aerolinea con mas vuelos C se filtran los vuelos C
-- luego se agrupa por aerolinea, seguido se ordena de forma descendiente por la columna count
-- y finalmente se limita al primer registro generando entonces la aerolinea con mas vuelos C

SELECT Aerolinea, count(*) from vv_vuelos where OnTime = 'C' group by Aerolinea order by 2 desc limit 1;

-- 3) Hung Cho vuela para las aerolineas
-- 
-- Para conseguir la aerolinea para la que trabaja Hung Cho,
-- Se filtra el nombre 'Hung Cho' en la clausula where
-- Luego se establece que entrege solo los valores distintos 
-- para el campo Piloto y finalmente obtenemos las aerolineas
-- para las cuales trabaja Hung Cho

SELECT distinct(Piloto), Aerolinea from vv_vuelos where Piloto = 'Hung Cho';

-- 4) Cuántos vuelos A, B, C tiene Chao Ma
-- 
-- Para conseguir los vuelos que tiene Chao Ma,
-- Se filtra el nombre 'Chao Ma' en la clausula where
-- Luego filtramos los vuelos A, B y C en el campo OnTime,
-- Luego se agrupa por los campos Piloto y OnTime para poder hacer uso del count 
-- y finalmente obtenemos el total de vuelos A, B y C que tuvo Chao Ma

SELECT Piloto, OnTime, count(*) from vv_vuelos where Piloto = 'Chao Ma' and OnTime in ('A', 'B', 'C') group by 1,2;
