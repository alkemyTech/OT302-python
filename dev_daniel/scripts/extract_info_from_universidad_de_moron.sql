-- UNIVERSIDAD DE MORÓN:
SELECT universidad AS university,
	   carrerra AS career,
	   fechaiscripccion AS inscription_date,
	   nombrre AS first_name,
	   sexo AS gender,
	   nacimiento AS age,
	   codgoposstal AS postal_code,
	   eemail AS email 
FROM moron_nacional_pampa
WHERE  
(TO_DATE(fechaiscripccion, 'DD/MM/YYYY')
BETWEEN '09-01-2020'
AND '02-01-2021')
AND universidad = 'Universidad de morón';


-- UNIVERSIDAD DE RÍO CUARTO:
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