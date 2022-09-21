SELECT 
	universidad AS university,
	carrera AS career,
	CAST(fecha_de_inscripcion  AS DATE) AS inscription_date,
	nombre,
	sexo AS gender,
	CAST(fecha_nacimiento AS DATE) AS date_birthday,
	localidad AS locations,
	email 
FROM
	salvador_villa_maria svm 
WHERE 
	universidad LIKE 'UNIVERSIDAD_DEL_SALVADOR'
	AND CAST(fecha_de_inscripcion AS DATE) BETWEEN '2020-09-01' AND '2021-02-01'