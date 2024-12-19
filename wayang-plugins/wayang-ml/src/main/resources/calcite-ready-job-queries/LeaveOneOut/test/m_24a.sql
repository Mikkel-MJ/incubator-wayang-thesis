SELECT MIN(chn.name) AS voiced_char_name, 
       MIN(n.name) AS voicing_actress_name, 
       MIN(t.title) AS voiced_action_movie_jap_eng
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
INNER JOIN postgres.movie_keyword AS mk ON mk.movie_id = t.id
INNER JOIN postgres.keyword AS k ON k.id = mk.keyword_id
WHERE ci.note IN ('(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)') 
     AND cn.country_code = '[us]' 
     AND it.info = 'release dates' 
     AND k.keyword IN ('hero', 'martial-arts', 'hand-to-hand-combat') 
     AND mi.info IS NOT NULL 
     AND (mi.info LIKE 'Japan:%201%' OR mi.info LIKE 'USA:%201%') 
     AND n.gender = 'f' 
     AND n.name LIKE '%An%' 
     AND rt.role = 'actress'
     AND ci.person_id = an.person_id 
     AND t.production_year > 2010;
