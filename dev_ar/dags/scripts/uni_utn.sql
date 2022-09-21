-- Universidad Tecnológica Nacional
-- All columns renamed 
-- inscription_date in date format
-- first_name and last_name already splitted
-- age, postal_code and location raw data from db

SELECT 
    utn.university AS university,
    utn.trabajo AS careers,
    TO_DATE(utn.inscription_date, 'YYYY/MM/DD') AS inscription_date,
    SPLIT_PART(REGEXP_REPLACE(utn.nombre, 'mrs\. |mr\. |dr\. ', ''), ' ', 1) AS first_name,
    SPLIT_PART(REGEXP_REPLACE(utn.nombre, 'mrs\. |mr\. |dr\. ', ''), ' ', 2) AS last_name,
    utn.sexo AS gender,
    utn.birth_date AS age,
    utn.location AS postal_code,
    utn.direccion AS location,
    utn.email AS email
FROM 
    jujuy_utn utn
WHERE 
    utn.university = 'universidad tecnológica nacional'
    AND TO_DATE(utn.inscription_date, 'YYYY/MM/DD') BETWEEN '2020-09-01' AND '2021-02-01';