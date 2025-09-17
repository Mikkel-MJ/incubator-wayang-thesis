SELECT MIN(mi.info) AS movie_budget,
       MIN(mi_idx.info) AS movie_votes,
       MIN(t.title) AS movie_title
FROM postgres.cast_info AS ci
INNER JOIN postgres.name AS n ON n.id = ci.person_id
INNER JOIN postgres.title AS t ON ci.movie_id = t.id
INNER JOIN postgres.movie_info AS mi ON t.id = mi.movie_id
INNER JOIN postgres.movie_info_idx AS mi_idx ON t.id = mi_idx.movie_id
INNER JOIN postgres.info_type AS it1 ON it1.id = mi.info_type_id
INNER JOIN postgres.info_type AS it2 ON it2.id = mi_idx.info_type_id
WHERE ci.note IN ('(writer)', '(head writer)', '(written by)', '(story)', '(story editor)')
    AND it1.info = 'genres' 
    AND it2.info = 'rating' 
    AND mi.info IN ('Horror', 'Thriller') 
    AND mi.note IS NULL 
    AND mi_idx.info > '8.0' 
    AND n.gender IS NOT NULL 
    AND n.gender = 'f' 
    AND t.production_year BETWEEN 2008 AND 2014;
