SELECT 
    MIN(chn.name) AS voiced_char, 
    MIN(n.name) AS voicing_actress, 
    MIN(t.title) AS voiced_animation
FROM 
    postgres.aka_name AS an
    INNER JOIN postgres.comp_cast_type AS cct1 ON cct1.kind = 'cast'
    INNER JOIN postgres.complete_cast AS cc ON cc.subject_id = cct1.id 
    INNER JOIN postgres.comp_cast_type AS cct2 ON cct2.kind = 'complete+verified'
    INNER JOIN postgres.name AS n ON n.id = ci.person_id
    INNER JOIN postgres.cast_info AS ci ON ci.person_id = n.id 
    INNER JOIN postgres.char_name AS chn ON chn.id = ci.person_role_id
    INNER JOIN postgres.company_name AS cn ON cn.id = mc.company_id
    INNER JOIN postgres.info_type AS it ON it.id = mi.info_type_id
    INNER JOIN postgres.info_type AS it3 ON it3.id = pi.info_type_id
    INNER JOIN postgres.keyword AS k ON k.id = mk.keyword_id
    INNER JOIN postgres.movie_companies AS mc ON mc.movie_id = t.id
    INNER JOIN postgres.movie_info AS mi ON mi.movie_id = t.id
    INNER JOIN postgres.movie_keyword AS mk ON mk.movie_id = t.id
    INNER JOIN postgres.person_info AS pi ON pi.person_id = n.id
    INNER JOIN postgres.role_type AS rt ON rt.id = ci.role_id
    INNER JOIN postgres.title AS t ON t.id = mi.movie_id
WHERE 
    ci.note IN ('(voice)', '(voice: japanese version)', '(voice) (uncredited)', '(voice: english version)')
    AND cn.country_code = '[us]'
    AND it.info = 'release dates'
    AND it3.info = 'trivia'
    AND k.keyword = 'computer-animation'
    AND mi.info IS NOT NULL
    AND (mi.info LIKE 'japan:%200%' OR mi.info LIKE 'usa:%200%')
    AND n.gender = 'f'
    AND n.name LIKE '%an%'
    AND rt.role = 'actress'
    AND cc.status_id = cct2.id
    AND ci.person_id = an.person_id 
    AND ci.person_id = pi.person_id
    AND t.production_year BETWEEN 2000 AND 2010;
    
