SELECT MIN(n.name) AS cast_member_name,
       MIN(pi.info) AS cast_member_info
FROM postgres.aka_name AS an
INNER JOIN postgres.name AS n ON n.id = an.person_id
INNER JOIN postgres.person_info AS pi ON pi.person_id = n.id
INNER JOIN postgres.cast_info AS ci ON ci.person_id = n.id
INNER JOIN postgres.title AS t ON t.id = ci.movie_id
INNER JOIN postgres.movie_link AS ml ON ml.linked_movie_id = t.id
INNER JOIN postgres.link_type AS lt ON lt.id = ml.link_type_id
INNER JOIN postgres.info_type AS it ON it.id = pi.info_type_id
WHERE an.name IS NOT NULL
  AND (an.name LIKE '%a%' OR an.name LIKE 'A%')
  AND it.info = 'mini biography'
  AND lt.link IN ('references', 'referenced in', 'features', 'featured in')
  AND n.name_pcode_cf BETWEEN 'A' AND 'F'
  AND (n.gender = 'm' OR (n.gender = 'f' AND n.name LIKE 'A%'))
  AND pi.note IS NOT NULL
  AND t.production_year BETWEEN 1980 AND 2010;
