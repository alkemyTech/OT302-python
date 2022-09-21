SELECT univiersities AS university,
	   carrera AS career,
	   inscription_dates AS inscription_date,
	   names AS first_name,
	   sexo AS gender,
	   fechas_nacimiento AS age,
	   localidad AS "location",
	   email AS email 
FROM rio_cuarto_interamericana 
WHERE  
(TO_DATE(inscription_dates, 'YY/Mon/DD')
BETWEEN '2020-09-01' 
AND '2021-02-01')
AND univiersities = 'Universidad-nacional-de-río-cuarto';