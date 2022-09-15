SELECT 
	universidad AS university,
	carrera AS career,
	fecha_de_inscripcion AS inscription_date,
	SUBSTRING("name" FROM '\w+' ) AS first_name,
	SUBSTRING("name" FROM '\w+$' ) AS last_name,
	sexo AS gender,
	fecha_nacimiento,
	CAST(fc.codigo_postal AS INTEGER) AS postal_code,
	l.localidad AS locations,
	correo_electronico AS email 
FROM
	flores_comahue fc
INNER JOIN localidad l
	ON CAST(fc.codigo_postal AS INTEGER) = l.codigo_postal
WHERE 
	universidad = 'UNIV. NACIONAL DEL COMAHUE'
	AND fecha_de_inscripcion BETWEEN '2020-09-01' AND '2021-02-01'