-- La columna de  location se tendrá más adelante según lo acordado, el procesamiento y la
--Limpieza se hará con Pandas, cualquiera se deja la columna Address si hay que utilizarla como
--llave primeria para la localidad, de lo contrario se omite en la limpieza de datos

SELECT 
			   univiersities                   AS university,
			   carrera                        AS career,
			   TO_DATE(inscription_dates,'YY mon DD')       AS inscription_date,
			   names    AS                   name,	  
			   sexo				               AS gender, 
			fechas_nacimiento AS age, 
			   direcciones                     AS location,
			   email     
			   
FROM   rio_cuarto_interamericana
WHERE univiersities ='-universidad-abierta-interamericana' AND 
TO_DATE (inscription_dates,'YY mon DD') BETWEEN '2020-09-01' AND '2021-02-01'







