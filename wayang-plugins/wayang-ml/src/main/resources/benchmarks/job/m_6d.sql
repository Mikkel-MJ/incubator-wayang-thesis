SELECT MIN(k.keyword) AS movie_keyword,
       MIN(n.name) AS actor_name,
       MIN(t.title) AS hero_movie
FROM postgres.cast_info AS ci
INNER JOIN postgres.movie_keyword AS mk ON ci.movie_id = mk.movie_id
INNER JOIN postgres.keyword AS k ON k.id = mk.keyword_id
INNER JOIN postgres.title AS t ON t.id = mk.movie_id
INNER JOIN postgres.name AS n ON n.id = ci.person_id
WHERE k.keyword IN ('superhero', 'sequel', 'second-part', 'marvel-comics', 'based-on-comic', 'tv-special', 'fight', 'violence')
  AND n.name LIKE '%Downey%Robert%'
  AND t.production_year > 2000;
