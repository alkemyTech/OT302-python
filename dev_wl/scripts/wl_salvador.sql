SELECT 
	universidad AS university,
	carrera AS career,
	CAST (fecha_de_inscripcion  AS DATE) AS inscription_date,
	nombre,
	SPLIT_PART(nombre, '_', 1) AS first_name,
	SPLIT_PART(nombre, '_', 2) AS last_name,
	sexo AS gender,
	cast (fecha_nacimiento AS DATE) AS date_birthday,
	l.codigo_postal AS postal_code,
	svm.localidad AS locations,
	email 
FROM
	salvador_villa_maria svm 
INNER JOIN localidad l
	ON svm.localidad  = l.localidad 
WHERE 
	universidad = 'UNIVERSIDAD_DEL_SALVADOR'
	AND CAST (fecha_de_inscripcion AS DATE) BETWEEN '2020-09-01' AND '2021-02-01'