SELECT MIN(lt.link) AS link_type,
       MIN(t1.title) AS first_movie,
       MIN(t2.title) AS second_movie
FROM postgres.title AS t1
INNER JOIN postgres.movie_keyword AS mk ON t1.id = mk.movie_id
INNER JOIN postgres.keyword AS k ON mk.keyword_id = k.id
INNER JOIN postgres.movie_link AS ml ON t1.id = ml.movie_id
INNER JOIN postgres.title AS t2 ON ml.linked_movie_id = t2.id
INNER JOIN postgres.link_type AS lt ON ml.link_type_id = lt.id
WHERE k.keyword = 'character-name-in-title';
