SELECT MIN(n.name) AS of_person,
       MIN(t.title) AS biography_movie
FROM postgres.aka_name AS an
INNER JOIN postgres.name AS n ON n.id = an.person_id
INNER JOIN postgres.person_info AS pi ON pi.person_id = n.id
INNER JOIN postgres.cast_info AS ci ON ci.person_id = n.id
INNER JOIN postgres.title AS t ON t.id = ci.movie_id
INNER JOIN postgres.movie_link AS ml ON ml.linked_movie_id = t.id
INNER JOIN postgres.link_type AS lt ON lt.id = ml.link_type_id
INNER JOIN postgres.info_type AS it ON it.id = pi.info_type_id
WHERE an.name LIKE '%a%'
  AND it.info = 'mini biography'
  AND lt.link = 'features'
  AND n.name_pcode_cf LIKE 'D%'
  AND n.gender = 'm'
  AND pi.note = 'Volker Boehm'
  AND t.production_year BETWEEN 1980 AND 1984;
