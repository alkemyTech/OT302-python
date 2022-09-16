SELECT 
	universidad AS university,
	carrera AS career,
	CAST(fecha_de_inscripcion  AS DATE) AS inscription_date,
	"name" AS name,
	sexo AS gender,
	CAST(fecha_nacimiento AS DATE) AS date_birthday,
	CAST(fc.codigo_postal AS INTEGER) AS postal_code,
	correo_electronico AS email 
FROM
	flores_comahue fc
WHERE 
	universidad LIKE 'UNIV. NACIONAL DEL COMAHUE'
	AND fecha_de_inscripcion BETWEEN '2020-09-01' AND '2021-02-01'