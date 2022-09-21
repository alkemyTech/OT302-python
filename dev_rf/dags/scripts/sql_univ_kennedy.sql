/* Things that must be corrected with Python
	- name: It has to be splited into first_name and last_name. If there is any dr, ms, etc has to be quited.
	- age: If the age is negative 100 must be added. Check if under 18 or above 70.
	- location: The Excel file with the postal_codes and locations has to be used to add the location to the table.
*/

SELECT 
universidades AS university,
carreras AS career,
CAST (fechas_de_inscripcion AS DATE) AS inscription_date,
nombres AS name,
sexo AS gender,
(CURRENT_DATE - TO_DATE(fechas_nacimiento, 'YY-Mon-DD'))/365 AS age,
codigos_postales AS postal_code,
emails AS email
FROM uba_kenedy
WHERE universidades = 'universidad-j.-f.-kennedy' AND 
CAST (fechas_de_inscripcion AS DATE) BETWEEN '2020-09-01' AND '2021-02-01'