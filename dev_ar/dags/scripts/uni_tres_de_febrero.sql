-- Universidad Tres de Febrero
-- All columns renamed [university, careers, inscription_date, first_name, last_name, gender, age, postal_code, location, email]
-- inscription_date in date format
-- first_name and last_name already splitted
-- age and location raw data from db

SELECT 
    untref.universidad AS university,
    untref.careers as careers,
    TO_DATE(untref.fecha_de_inscripcion, 'DD/Mon/YY') AS inscription_date,
    SPLIT_PART(REGEXP_REPLACE(untref.names, 'mrs\._|mr\._|dr\._|', ''), '_', 1) AS first_name,
    SPLIT_PART(REGEXP_REPLACE(untref.names, 'mrs\._|mr\._|dr\._|', ''), '_', 2) AS last_name,
    untref.sexo AS gender,
    untref.birth_dates AS age,
    untref.codigo_postal AS postal_code,
    untref.direcciones AS location,
    untref.correos_electronicos AS email
FROM 
    palermo_tres_de_febrero untref
WHERE 
    untref.universidad = 'universidad_nacional_de_tres_de_febrero'
    AND TO_DATE(untref.fecha_de_inscripcion, 'DD/Mon/YY') BETWEEN '2020-09-01' AND '2021-02-01';