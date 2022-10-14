select uba.universidades "university",
       uba.carreras "career",
       uba.fechas_de_inscripcion::date "inscription_date",
       regexp_replace(uba.nombres, '^([a-z]+)-.*', '\1', 'g') "first_name",
       regexp_replace(uba.nombres, '.*-([a-z]+)','\1', 'g') "last_name",
       uba.emails "email",
       uba.sexo "gender",
       to_date(uba.fechas_nacimiento, 'yy-Mon-dd') "birth_dates",
       uba.codigos_postales "postal_code"
from uba_kenedy as uba
where uba.universidades = 'universidad-de-buenos-aires' and uba.fechas_de_inscripcion::date between '2020-09-01' and '2021-02-01';
select l.universities "university",
       l.careers "career",
       to_date(l.inscription_dates, 'DD-MM-YYYY') "inscription_date",
       regexp_replace(l.names, '^([A-Z]+\.-)?([A-Z]+)-.*', '\2', 'g') "first_name",
       regexp_replace(l.names, '.*-([A-Z]+)$','\1', 'g') "last_name",
       l.emails "email",
       l.sexo "gender",
       to_date(l.birth_dates, 'DD-MM-YYYY') "birth_dates",
       l.locations "locations"
from lat_sociales_cine as l
where l.universities = 'UNIVERSIDAD-DEL-CINE' and to_date(l.inscription_dates, 'DD-MM-YYYY') between '2020-09-01' and '2021-02-01';