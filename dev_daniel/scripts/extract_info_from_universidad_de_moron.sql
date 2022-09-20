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