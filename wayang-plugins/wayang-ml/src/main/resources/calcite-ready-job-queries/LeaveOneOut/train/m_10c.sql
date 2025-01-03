SELECT MIN(chn.name) AS character_name,
       MIN(t.title) AS movie_with_american_producer
FROM postgres.char_name AS chn
INNER JOIN postgres.cast_info AS ci ON chn.id = ci.person_role_id
INNER JOIN postgres.title AS t ON ci.movie_id = t.id
INNER JOIN postgres.movie_companies AS mc ON t.id = mc.movie_id
INNER JOIN postgres.company_name AS cn ON mc.company_id = cn.id
INNER JOIN postgres.company_type AS ct ON mc.company_type_id = ct.id
INNER JOIN postgres.role_type AS rt ON ci.role_id = rt.id
WHERE ci.note LIKE '%(producer)%'
  AND cn.country_code = '[us]'
  AND t.production_year > 1990;
