select l.universities "university",
       l.careers "career",
       to_date(l.inscription_dates, 'DD-MM-YYYY') "inscription_date",
       regexp_replace(l.names, '^([A-Z]{2}\.-)?([A-Z]+)-.*', '\2', 'g') "first_name",
       regexp_replace(l.names, '.*-([A-Z]+)$','\1', 'g') "last_name",
       l.emails "email",
       l.sexo "gemder",
       date_part('year',age(current_date, to_date(l.birth_dates, 'DD-MM-YYYY'))) "age",
       l.locations
from lat_sociales_cine as l;