SELECT MIN(a1.name) AS writer_pseudo_name,
       MIN(t.title) AS movie_title
FROM postgres.aka_name AS a1
INNER JOIN postgres.name AS n1 ON a1.person_id = n1.id
INNER JOIN postgres.cast_info AS ci ON n1.id = ci.person_id
INNER JOIN postgres.title AS t ON ci.movie_id = t.id
INNER JOIN postgres.movie_companies AS mc ON t.id = mc.movie_id
INNER JOIN postgres.company_name AS cn ON mc.company_id = cn.id
INNER JOIN postgres.role_type AS rt ON ci.role_id = rt.id
WHERE cn.country_code = '[us]'
  AND rt.role = 'writer';
