select uba.universidades "university",
       uba.carreras "career",
       uba.fechas_de_inscripcion::date "inscription_date",
       regexp_replace(uba.nombres, '([a-z]+)-.*', '\1', 'g') "first_name",
       regexp_replace(uba.nombres, '.*-([a-z]+)','\1', 'g') "last_name",
       uba.emails "email",
       uba.sexo "gemder",
       date_part('year',age(current_date,to_date(uba.fechas_nacimiento, 'yy-Mon-dd'))) "age",
       uba.codigos_postales "location"
from uba_kenedy as uba
where uba.universidades = 'universidad-de-buenos-aires' and uba.fechas_de_inscripcion::date between '2020-09-01' and '2021-02-01' order by uba.fechas_de_inscripcion::date;

select l.universities "university",
       l.careers "career",
       to_date(l.inscription_dates, 'DD-MM-YYYY') "inscription_date",
       regexp_replace(l.names, '^([A-Z]+\.-)?([A-Z]+)-.*', '\2', 'g') "first_name",
       regexp_replace(l.names, '.*-([A-Z]+)$','\1', 'g') "last_name",
       l.emails "email",
       l.sexo "gemder",
       date_part('year',age(current_date, to_date(l.birth_dates, 'DD-MM-YYYY'))) "age",
       l.locations
from lat_sociales_cine as l;