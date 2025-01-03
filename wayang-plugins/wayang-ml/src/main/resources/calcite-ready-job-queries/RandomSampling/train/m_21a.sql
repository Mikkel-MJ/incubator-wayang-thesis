SELECT MIN(cn.name) AS company_name, 
       MIN(lt.link) AS link_type, 
       MIN(t.title) AS western_follow_up
FROM postgres.company_name AS cn
INNER JOIN postgres.movie_companies AS mc ON mc.company_id = cn.id
INNER JOIN postgres.company_type AS ct ON mc.company_type_id = ct.id
INNER JOIN postgres.movie_info AS mi ON mi.movie_id = mc.movie_id
INNER JOIN postgres.title AS t ON t.id = mi.movie_id
INNER JOIN postgres.movie_keyword AS mk ON mk.movie_id = t.id
INNER JOIN postgres.keyword AS k ON k.id = mk.keyword_id
INNER JOIN postgres.movie_link AS ml ON ml.movie_id = t.id
INNER JOIN postgres.link_type AS lt ON lt.id = ml.link_type_id
WHERE cn.country_code <> '[pl]' 
    AND (cn.name LIKE '%Film%' OR cn.name LIKE '%Warner%') 
    AND ct.kind = 'production companies' 
    AND k.keyword = 'sequel' 
    AND lt.link LIKE '%follow%' 
    AND mc.note IS NULL 
    AND mi.info IN ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Denish', 'Norwegian', 'German') 
    AND t.production_year BETWEEN 1950 AND 2000;
