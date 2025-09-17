SELECT MIN(mi.info) AS movie_budget,
       MIN(mi_idx.info) AS movie_votes,
       MIN(n.name) AS male_writer,
       MIN(t.title) AS violent_movie_title
FROM postgres.cast_info ci
INNER JOIN postgres.title t ON t.id = ci.movie_id
INNER JOIN postgres.movie_info mi ON t.id = mi.movie_id
INNER JOIN postgres.movie_info_idx mi_idx ON t.id = mi_idx.movie_id
INNER JOIN postgres.movie_keyword mk ON t.id = mk.movie_id
INNER JOIN postgres.keyword k ON k.id = mk.keyword_id
INNER JOIN postgres.name n ON n.id = ci.person_id
INNER JOIN postgres.info_type it1 ON it1.id = mi.info_type_id
INNER JOIN postgres.info_type it2 ON it2.id = mi_idx.info_type_id
WHERE ci.note IN ('(writer)', '(head writer)', '(written by)', '(story)', '(story editor)')
  AND it1.info = 'genres'
  AND it2.info = 'votes'
  AND k.keyword IN ('murder', 'violence', 'blood', 'gore', 'death', 'female-nudity', 'hospital')
  AND mi.info IN ('Horror', 'Action', 'Sci-Fi', 'Thriller', 'Crime', 'War')
  AND n.gender = 'm';
