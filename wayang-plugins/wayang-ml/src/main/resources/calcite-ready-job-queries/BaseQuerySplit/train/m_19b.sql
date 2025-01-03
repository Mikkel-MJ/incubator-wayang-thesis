SELECT MIN(n.name) AS voicing_actress, MIN(t.title) AS kung_fu_panda
FROM postgres.cast_info AS ci
INNER JOIN postgres.name AS n ON n.id = ci.person_id
INNER JOIN postgres.title AS t ON ci.movie_id = t.id
INNER JOIN postgres.movie_info AS mi ON t.id = mi.movie_id
INNER JOIN postgres.movie_companies AS mc ON t.id = mc.movie_id
INNER JOIN postgres.company_name AS cn ON mc.company_id = cn.id
INNER JOIN postgres.info_type AS it ON it.id = mi.info_type_id
INNER JOIN postgres.role_type AS rt ON rt.id = ci.role_id
INNER JOIN postgres.aka_name AS an ON n.id = an.person_id
INNER JOIN postgres.char_name AS chn ON chn.id = ci.person_role_id
WHERE ci.note = '(voice)' 
    AND cn.country_code = '[us]' 
    AND it.info = 'release dates' 
    AND mc.note LIKE '%(200%)%' 
    AND (mc.note LIKE '%(USA)%' OR mc.note LIKE '%(worldwide)%') 
    AND mi.info IS NOT NULL 
    AND (mi.info LIKE 'Japan:%2007%' OR mi.info LIKE 'USA:%2008%') 
    AND n.gender = 'f' 
    AND n.name LIKE '%Angel%' 
    AND rt.role = 'actress' 
    AND t.production_year BETWEEN 2007 AND 2008 
    AND ci.person_id = an.person_id
    AND t.title LIKE '%Kung%Fu%Panda%';
