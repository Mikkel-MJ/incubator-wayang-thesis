SELECT MIN(an.name) AS acress_pseudonym,
       MIN(t.title) AS japanese_anime_movie
FROM postgres.aka_name AS an
INNER JOIN postgres.name AS n ON an.person_id = n.id
INNER JOIN postgres.cast_info AS ci ON n.id = ci.person_id
INNER JOIN postgres.title AS t ON ci.movie_id = t.id
INNER JOIN postgres.movie_companies AS mc ON t.id = mc.movie_id
INNER JOIN postgres.company_name AS cn ON mc.company_id = cn.id
INNER JOIN postgres.role_type AS rt ON ci.role_id = rt.id
WHERE ci.note = '(voice: English version)'
  AND cn.country_code = '[jp]'
  AND mc.note LIKE '%(Japan)%'
  AND mc.note NOT LIKE '%(USA)%'
  AND (mc.note LIKE '%(2006)%' OR mc.note LIKE '%(2007)%')
  AND n.name LIKE '%Yo%'
  AND n.name NOT LIKE '%Yu%'
  AND rt.role = 'actress'
  AND t.production_year BETWEEN 2006 AND 2007
  AND (t.title LIKE 'One Piece%' OR t.title LIKE 'Dragon Ball Z%');
