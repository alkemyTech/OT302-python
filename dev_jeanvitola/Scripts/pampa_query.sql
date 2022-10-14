-- La Columna de localidad se hará mas adelante según lo acordado

SELECT 
       universidad                     AS university,
       carrerra                        AS career,
       fechaiscripccion                AS inscription_date,
       nombrre                         AS name,
       sexo                            AS gender,
       nacimiento                      AS age,
       codgoposstal                    AS postal_code,
       eemail                          AS email
FROM   moron_nacional_pampa
WHERE  TO_DATE(fechaiscripccion,'DD MM YYYY') BETWEEN '2020-09-01' AND '2021-02-01' AND
		universidad = 'Universidad nacional de la pampa'


