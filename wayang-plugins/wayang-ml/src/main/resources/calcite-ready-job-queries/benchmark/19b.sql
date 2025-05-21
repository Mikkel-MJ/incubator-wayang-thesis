SELECT MIN(n.name) AS voicing_actress,
       MIN(t.title) AS kung_fu_panda
FROM postgres.aka_name AS an,
     postgres.char_name AS chn,
     postgres.cast_info AS ci,
     postgres.company_name AS cn,
     postgres.info_type AS it,
     postgres.movie_companies AS mc,
     postgres.movie_info AS mi,
     postgres.name AS n,
     postgres.role_type AS rt,
     postgres.title AS t
WHERE ci.note = '(voice)'
  AND cn.country_code ='[us]'
  AND it.info = 'release dates'
  AND mc.note LIKE '%(200%)%'
  AND (mc.note LIKE '%(USA)%'
       OR mc.note LIKE '%(worldwide)%')
  AND mi.info IS NOT NULL
  AND (mi.info LIKE 'Japan:%2007%'
       OR mi.info LIKE 'USA:%2008%')
  AND n.gender ='f'
  AND n.name LIKE '%Angel%'
  AND rt.role ='actress'
  AND t.production_year BETWEEN 2007 AND 2008
  AND t.title LIKE '%Kung%Fu%Panda%'
  AND t.id = mi.movie_id
  AND t.id = mc.movie_id
  AND t.id = ci.movie_id
  AND mc.movie_id = ci.movie_id
  AND mc.movie_id = mi.movie_id
  AND mi.movie_id = ci.movie_id
  AND cn.id = mc.company_id
  AND it.id = mi.info_type_id
  AND n.id = ci.person_id
  AND rt.id = ci.role_id
  AND n.id = an.person_id
  AND ci.person_id = an.person_id
  AND chn.id = ci.person_role_id;

