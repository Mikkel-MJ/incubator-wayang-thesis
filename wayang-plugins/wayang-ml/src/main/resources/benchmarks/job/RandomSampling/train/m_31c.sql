SELECT MIN(mi.info) AS movie_budget,
       MIN(mi_idx.info) AS movie_votes,
       MIN(n.name) AS writer,
       MIN(t.title) AS violent_liongate_movie
FROM postgres.cast_info AS ci
INNER JOIN postgres.movie_companies AS mc ON mc.movie_id = ci.movie_id
INNER JOIN postgres.company_name AS cn ON cn.id = mc.company_id
INNER JOIN postgres.movie_info AS mi ON mi.movie_id = ci.movie_id
INNER JOIN postgres.info_type AS it1 ON it1.id = mi.info_type_id
INNER JOIN postgres.movie_info_idx AS mi_idx ON mi_idx.movie_id = ci.movie_id
INNER JOIN postgres.info_type AS it2 ON it2.id = mi_idx.info_type_id
INNER JOIN postgres.movie_keyword AS mk ON mk.movie_id = ci.movie_id
INNER JOIN postgres.keyword AS k ON k.id = mk.keyword_id
INNER JOIN postgres.name AS n ON n.id = ci.person_id
INNER JOIN postgres.title AS t ON t.id = ci.movie_id
WHERE ci.note IN ('(writer)', '(head writer)', '(written by)', '(story)', '(story editor)')
  AND cn.name LIKE 'Lionsgate%'
  AND it1.info = 'genres'
  AND it2.info = 'votes'
  AND k.keyword IN ('murder', 'violence', 'blood', 'gore', 'death', 'female-nudity', 'hospital')
  AND mi.info IN ('Horror', 'Action', 'Sci-Fi', 'Thriller', 'Crime', 'War')
  AND t.id = mi.movie_id
  AND t.id = mi_idx.movie_id
  AND t.id = ci.movie_id
  AND t.id = mk.movie_id
  AND t.id = mc.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND ci.movie_id = mk.movie_id
  AND ci.movie_id = mc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mk.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi_idx.movie_id = mk.movie_id
  AND mi_idx.movie_id = mc.movie_id
  AND mk.movie_id = mc.movie_id
  AND n.id = ci.person_id
  AND it1.id = mi.info_type_id
  AND it2.id = mi_idx.info_type_id
  AND k.id = mk.keyword_id
  AND cn.id = mc.company_id;
