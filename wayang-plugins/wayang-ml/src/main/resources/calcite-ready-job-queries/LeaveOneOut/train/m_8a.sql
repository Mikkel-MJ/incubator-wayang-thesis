SELECT MIN(an1.name) AS actress_pseudonym,
       MIN(t.title) AS japanese_movie_dubbed
FROM postgres.title AS t
INNER JOIN postgres.cast_info AS ci ON t.id = ci.movie_id
INNER JOIN postgres.movie_companies AS mc ON t.id = mc.movie_id
INNER JOIN postgres.company_name AS cn ON mc.company_id = cn.id
INNER JOIN postgres.name AS n1 ON n1.id = ci.person_id
INNER JOIN postgres.aka_name AS an1 ON an1.person_id = n1.id
INNER JOIN postgres.role_type AS rt ON ci.role_id = rt.id
WHERE ci.note = '(voice: English version)'
  AND cn.country_code = '[jp]'
  AND mc.note LIKE '%(Japan)%'
  AND mc.note NOT LIKE '%(USA)%'
  AND n1.name LIKE '%Yo%'
  AND n1.name NOT LIKE '%Yu%'
  AND rt.role = 'actress'
  AND an1.person_id = ci.person_id
  AND ci.movie_id = mc.movie_id;
