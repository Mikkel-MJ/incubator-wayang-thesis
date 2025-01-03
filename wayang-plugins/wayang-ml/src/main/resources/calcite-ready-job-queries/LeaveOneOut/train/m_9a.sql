SELECT MIN(an.name) AS alternative_name,
       MIN(chn.name) AS character_name,
       MIN(t.title) AS movie
FROM postgres.title AS t
INNER JOIN postgres.cast_info AS ci ON t.id = ci.movie_id
INNER JOIN postgres.movie_companies AS mc ON t.id = mc.movie_id
INNER JOIN postgres.company_name AS cn ON mc.company_id = cn.id
INNER JOIN postgres.name AS n ON n.id = ci.person_id
INNER JOIN postgres.aka_name AS an ON an.person_id = n.id
INNER JOIN postgres.char_name AS chn ON chn.id = ci.person_role_id
INNER JOIN postgres.role_type AS rt ON ci.role_id = rt.id
WHERE ci.note IN ('(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)')
  AND cn.country_code = '[us]'
  AND mc.note IS NOT NULL
  AND (mc.note LIKE '%(USA)%' OR mc.note LIKE '%(worldwide)%')
  AND n.gender = 'f'
  AND n.name LIKE '%Ang%'
  AND rt.role = 'actress'
  AND t.production_year BETWEEN 2005 AND 2015;
