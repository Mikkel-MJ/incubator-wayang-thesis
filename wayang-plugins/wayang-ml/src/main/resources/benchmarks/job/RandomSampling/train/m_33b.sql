SELECT MIN(cn1.name) AS first_company,
       MIN(cn2.name) AS second_company,
       MIN(mi_idx1.info) AS first_rating,
       MIN(mi_idx2.info) AS second_rating,
       MIN(t1.title) AS first_movie,
       MIN(t2.title) AS second_movie
FROM postgres.company_name AS cn1
INNER JOIN postgres.movie_companies AS mc1 ON cn1.id = mc1.company_id
INNER JOIN postgres.title AS t1 ON t1.id = mc1.movie_id
INNER JOIN postgres.movie_info_idx AS mi_idx1 ON t1.id = mi_idx1.movie_id
INNER JOIN postgres.info_type AS it1 ON it1.id = mi_idx1.info_type_id
INNER JOIN postgres.kind_type AS kt1 ON kt1.id = t1.kind_id
INNER JOIN postgres.movie_link AS ml ON t1.id = ml.movie_id
INNER JOIN postgres.link_type AS lt ON lt.id = ml.link_type_id
INNER JOIN postgres.title AS t2 ON t2.id = ml.linked_movie_id
INNER JOIN postgres.movie_info_idx AS mi_idx2 ON t2.id = mi_idx2.movie_id
INNER JOIN postgres.info_type AS it2 ON it2.id = mi_idx2.info_type_id
INNER JOIN postgres.kind_type AS kt2 ON kt2.id = t2.kind_id
INNER JOIN postgres.movie_companies AS mc2 ON t2.id = mc2.movie_id
INNER JOIN postgres.company_name AS cn2 ON cn2.id = mc2.company_id
WHERE cn1.country_code = '[nl]'
  AND it1.info = 'rating'
  AND it2.info = 'rating'
  AND kt1.kind = 'tv series'
  AND kt2.kind = 'tv series'
  AND lt.link LIKE '%follow%'
  AND mi_idx2.info < '3.0'
  AND t2.production_year = 2007;
