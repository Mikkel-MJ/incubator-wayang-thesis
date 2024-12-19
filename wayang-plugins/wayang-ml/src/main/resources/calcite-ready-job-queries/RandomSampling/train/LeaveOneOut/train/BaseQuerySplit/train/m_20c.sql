SELECT MIN(n.name) AS cast_member, 
       MIN(t.title) AS complete_dynamic_hero_movie
FROM postgres.complete_cast AS cc
INNER JOIN postgres.comp_cast_type AS cct1 ON cct1.id = cc.subject_id
INNER JOIN postgres.comp_cast_type AS cct2 ON cct2.id = cc.status_id
INNER JOIN postgres.cast_info AS ci ON ci.movie_id = cc.movie_id
INNER JOIN postgres.char_name AS chn ON chn.id = ci.person_role_id
INNER JOIN postgres.movie_keyword AS mk ON mk.movie_id = ci.movie_id
INNER JOIN postgres.keyword AS k ON k.id = mk.keyword_id
INNER JOIN postgres.title AS t ON t.id = ci.movie_id
INNER JOIN postgres.kind_type AS kt ON kt.id = t.kind_id
INNER JOIN postgres.name AS n ON n.id = ci.person_id
WHERE cct1.kind = 'cast' 
    AND cct2.kind LIKE '%complete%' 
    AND chn.name IS NOT NULL AND (chn.name LIKE '%man%' OR chn.name LIKE '%Man%') 
    AND k.keyword IN ('superhero', 'marvel-comics', 'based-on-comic', 'tv-special', 'fight', 'violence', 'magnet', 'web', 'claw', 'laser') 
    AND kt.kind = 'movie' 
    AND t.production_year > 2000;
