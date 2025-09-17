SELECT MIN(chn.name) AS uncredited_voiced_character,
       MIN(t.title) AS russian_movie
FROM postgres.title AS t
INNER JOIN postgres.cast_info AS ci ON t.id = ci.movie_id
INNER JOIN postgres.movie_companies AS mc ON t.id = mc.movie_id
INNER JOIN postgres.company_name AS cn ON mc.company_id = cn.id
INNER JOIN postgres.company_type AS ct ON mc.company_type_id = ct.id
INNER JOIN postgres.char_name AS chn ON chn.id = ci.person_role_id
INNER JOIN postgres.role_type AS rt ON ci.role_id = rt.id
WHERE ci.note LIKE '%(voice)%'
  AND ci.note LIKE '%(uncredited)%'
  AND cn.country_code = '[ru]'
  AND rt.role = 'actor'
  AND t.production_year > 2005;
