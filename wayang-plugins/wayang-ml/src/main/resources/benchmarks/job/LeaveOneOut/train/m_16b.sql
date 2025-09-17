SELECT MIN(an.name) AS cool_actor_pseudonym,
       MIN(t.title) AS series_named_after_char
FROM postgres.title AS t
INNER JOIN postgres.cast_info AS ci ON t.id = ci.movie_id
INNER JOIN postgres.movie_keyword AS mk ON t.id = mk.movie_id
INNER JOIN postgres.movie_companies AS mc ON t.id = mc.movie_id
INNER JOIN postgres.aka_name AS an ON an.person_id = ci.person_id
INNER JOIN postgres.name AS n ON an.person_id = n.id
INNER JOIN postgres.keyword AS k ON mk.keyword_id = k.id
INNER JOIN postgres.company_name AS cn ON mc.company_id = cn.id
WHERE cn.country_code = '[us]'
  AND k.keyword = 'character-name-in-title';
