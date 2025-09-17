SELECT MIN(n.name) AS member_in_charnamed_movie
FROM postgres.cast_info AS ci
INNER JOIN postgres.name AS n ON n.id = ci.person_id
INNER JOIN postgres.title AS t ON ci.movie_id = t.id
INNER JOIN postgres.movie_keyword AS mk ON t.id = mk.movie_id
INNER JOIN postgres.keyword AS k ON mk.keyword_id = k.id
INNER JOIN postgres.movie_companies AS mc ON t.id = mc.movie_id
INNER JOIN postgres.company_name AS cn ON mc.company_id = cn.id
WHERE k.keyword = 'character-name-in-title' 
  AND n.name LIKE '%B%';
