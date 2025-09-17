SELECT MIN(an1.name) AS costume_designer_pseudo,
       MIN(t.title) AS movie_with_costumes
FROM postgres.aka_name AS an1,
     postgres.cast_info AS ci,
     postgres.company_name AS cn,
     postgres.movie_companies AS mc,
     postgres.name AS n1,
     postgres.role_type AS rt,
     postgres.title AS t
WHERE cn.country_code ='[us]'
  AND rt.role ='costume designer'
  AND an1.person_id = n1.id
  AND n1.id = ci.person_id
  AND ci.movie_id = t.id
  AND t.id = mc.movie_id
  AND mc.company_id = cn.id
  AND ci.role_id = rt.id
  AND an1.person_id = ci.person_id
  AND ci.movie_id = mc.movie_id;

