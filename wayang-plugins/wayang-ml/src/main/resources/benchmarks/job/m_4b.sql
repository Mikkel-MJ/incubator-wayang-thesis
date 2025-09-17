SELECT MIN(mi_idx.info) AS rating,
       MIN(t.title) AS movie_title
FROM postgres.info_type AS it
INNER JOIN postgres.movie_info_idx AS mi_idx ON it.id = mi_idx.info_type_id
INNER JOIN postgres.title AS t ON t.id = mi_idx.movie_id
INNER JOIN postgres.movie_keyword AS mk ON t.id = mk.movie_id
INNER JOIN postgres.keyword AS k ON k.id = mk.keyword_id
WHERE it.info = 'rating'
  AND k.keyword LIKE '%sequel%'
  AND mi_idx.info > '9.0'
  AND t.production_year > 2010;
