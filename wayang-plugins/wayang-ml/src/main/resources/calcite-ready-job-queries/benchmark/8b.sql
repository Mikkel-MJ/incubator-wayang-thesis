SELECT MIN(an.name) AS acress_pseudonym,
       MIN(t.title) AS japanese_anime_movie
FROM postgres.aka_name AS an,
     postgres.cast_info AS ci,
     postgres.company_name AS cn,
     postgres.movie_companies AS mc,
     postgres.name AS n,
     postgres.role_type AS rt,
     postgres.title AS t
WHERE ci.note ='(voice: English version)'
  AND cn.country_code ='[jp]'
  AND mc.note LIKE '%(Japan)%'
  AND mc.note NOT LIKE '%(USA)%'
  AND (mc.note LIKE '%(2006)%'
       OR mc.note LIKE '%(2007)%')
  AND n.name LIKE '%Yo%'
  AND n.name NOT LIKE '%Yu%'
  AND rt.role ='actress'
  AND t.production_year BETWEEN 2006 AND 2007
  AND (t.title LIKE 'One Piece%'
       OR t.title LIKE 'Dragon Ball Z%')
  AND an.person_id = n.id
  AND n.id = ci.person_id
  AND ci.movie_id = t.id
  AND t.id = mc.movie_id
  AND mc.company_id = cn.id
  AND ci.role_id = rt.id
  AND an.person_id = ci.person_id
  AND ci.movie_id = mc.movie_id;

