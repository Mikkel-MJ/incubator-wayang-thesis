SELECT MIN(n.name) AS of_person,
       MIN(t.title) AS biography_movie
FROM postgres.title AS t
INNER JOIN postgres.cast_info AS ci ON t.id = ci.movie_id
INNER JOIN postgres.movie_link AS ml ON t.id = ml.linked_movie_id
INNER JOIN postgres.link_type AS lt ON lt.id = ml.link_type_id
INNER JOIN postgres.name AS n ON n.id = ci.person_id
INNER JOIN postgres.aka_name AS an ON n.id = an.person_id
INNER JOIN postgres.person_info AS pi ON n.id = pi.person_id
INNER JOIN postgres.info_type AS it ON it.id = pi.info_type_id
WHERE an.name LIKE '%a%'
  AND it.info = 'mini biography'
  AND lt.link = 'features'
  AND n.name_pcode_cf BETWEEN 'A' AND 'F'
  AND (n.gender = 'm' OR (n.gender = 'f' AND n.name LIKE 'B%'))
  AND pi.note = 'Volker Boehm'
  AND t.production_year BETWEEN 1980 AND 1995;
