SELECT MIN(kt.kind) AS movie_kind, 
       MIN(t.title) AS complete_nerdy_internet_movie
FROM postgres.complete_cast AS cc
INNER JOIN postgres.comp_cast_type AS cct1 ON cct1.id = cc.status_id
INNER JOIN postgres.company_name AS cn ON cn.id = mc.company_id
INNER JOIN postgres.company_type AS ct ON ct.id = mc.company_type_id
INNER JOIN postgres.info_type AS it1 ON it1.id = mi.info_type_id
INNER JOIN postgres.keyword AS k ON k.id = mk.keyword_id
INNER JOIN postgres.kind_type AS kt ON kt.id = t.kind_id
INNER JOIN postgres.movie_companies AS mc ON mc.movie_id = t.id
INNER JOIN postgres.movie_info AS mi ON mi.movie_id = t.id
INNER JOIN postgres.movie_keyword AS mk ON mk.movie_id = t.id
INNER JOIN postgres.movie_link AS ml ON ml.movie_id = t.id
INNER JOIN postgres.title AS t ON t.id = mi.movie_id
WHERE cct1.kind = 'complete+verified' 
    AND cn.country_code = '[us]' 
    AND it1.info = 'release dates' 
    AND k.keyword IN ('nerd', 'loner', 'alienation', 'dignity') 
    AND kt.kind IN ('movie') 
    AND mi.note LIKE '%internet%' 
    AND mi.info LIKE 'USA:% 200%' 
    AND t.production_year > 2000;
