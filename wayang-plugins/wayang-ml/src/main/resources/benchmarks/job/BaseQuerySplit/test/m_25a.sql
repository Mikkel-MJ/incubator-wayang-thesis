SELECT MIN(mi.info) AS movie_budget, 
       MIN(mi_idx.info) AS movie_votes, 
       MIN(n.name) AS male_writer, 
       MIN(t.title) AS violent_movie_title
FROM postgres.cast_info AS ci
INNER JOIN postgres.name AS n ON n.id = ci.person_id
INNER JOIN postgres.title AS t ON ci.movie_id = t.id
INNER JOIN postgres.movie_info AS mi ON t.id = mi.movie_id
INNER JOIN postgres.movie_info_idx AS mi_idx ON t.id = mi_idx.movie_id
INNER JOIN postgres.movie_keyword AS mk ON t.id = mk.movie_id
INNER JOIN postgres.keyword AS k ON k.id = mk.keyword_id
INNER JOIN postgres.info_type AS it1 ON it1.id = mi.info_type_id
INNER JOIN postgres.info_type AS it2 ON it2.id = mi_idx.info_type_id
WHERE ci.note IN ('(writer)', '(head writer)', '(written by)', '(story)', '(story editor)') 
    AND it1.info = 'genres' 
    AND it2.info = 'votes' 
    AND k.keyword IN ('murder', 'blood', 'gore', 'death', 'female-nudity') 
    AND mi.info = 'Horror' 
    AND n.gender = 'm';
