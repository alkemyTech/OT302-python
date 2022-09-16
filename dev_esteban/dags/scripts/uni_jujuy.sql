SELECT 
"university", "career",
"inscription_date",
"nombre",
REGEXP_REPLACE("nombre", '\s+\S+$', '') AS firstname,
REGEXP_REPLACE("nombre", '^.*\s+(\S+)$', '\1') AS lastname,
"sexo",
EXTRACT(YEAR FROM age(CAST("birth_date" as DATE))) as "age", 
"direccion", "location", "email"
FROM jujuy_utn
WHERE CAST("inscription_date" AS DATE)
BETWEEN TO_DATE('2020/09/01','YYYY/MM/DD') 
    AND TO_DATE('2021/02/01','YYYY/MM/DD');