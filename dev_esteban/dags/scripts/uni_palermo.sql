SELECT 
"universidad", "careers",
"fecha_de_inscripcion",
"names",
REGEXP_REPLACE("names",'[djmrs]*[\.]_*','') AS "curate_name",
"sexo", "birth_dates",
EXTRACT(YEAR FROM age(CAST("birth_dates" as DATE))) as "age",
"codigo_postal", 
"correos_electronicos"
FROM palermo_tres_de_febrero
WHERE CAST("fecha_de_inscripcion" AS DATE)
BETWEEN TO_DATE('01/Sep/20','DD/Mon/YY')
    AND TO_DATE('01/Feb/21','DD/Mon/YY');