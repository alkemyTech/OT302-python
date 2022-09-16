/* Things that must be corrected with Python
	- name: It has to be splited into first_name and last_name. If there is any dr, ms, etc has to be quited.
	- age: Check if under 18 or above 70.
	- postal_code: The Excel file with the postal_codes and locations has to be used to add the postal_code to the table.
*/

SELECT 
universities AS university,
careers AS career,
TO_DATE(inscription_dates, 'DD-MM-YYYY') AS inscription_date,
names AS name,
sexo AS gender,
(CURRENT_DATE - TO_DATE(birth_dates, 'DD-MM-YYYY'))/365 AS age,
locations AS location,
emails AS email
FROM lat_sociales_cine
WHERE universities = '-FACULTAD-LATINOAMERICANA-DE-CIENCIAS-SOCIALES' AND 
TO_DATE(inscription_dates, 'DD-MM-YYYY') BETWEEN '2020-09-01' AND '2021-02-01'