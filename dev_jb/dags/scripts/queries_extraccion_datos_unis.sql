-- Tabla datos_uni_flores --
SELECT  universidad,
		carrera,
		fecha_de_inscripcion,
		name AS nombre,
		sexo,
		fecha_nacimiento,
		codigo_postal,
		direccion,
		correo_electronico AS email
FROM flores_comahue fc 
WHERE universidad  LIKE 'UNIVERSIDAD DE FLORES'
AND fecha_de_inscripcion BETWEEN '2020-09-01' AND '2021-02-01';


-- Tabla datos_uni_villa_maria --
SELECT  universidad,
		carrera,
		fecha_de_inscripcion, 
		nombre,
		sexo,
		fecha_nacimiento, 
		direccion,
		email
FROM salvador_villa_maria sv
WHERE universidad LIKE 'UNIVERSIDAD_NACIONAL_DE_VILLA_MARÍA'
AND cast(fecha_de_inscripcion AS date) BETWEEN '2020-09-01' AND '2021-02-01';