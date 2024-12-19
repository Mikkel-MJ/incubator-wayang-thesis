SELECT MIN(cn.name) AS from_company,
       MIN(mc.note) AS production_note,
       MIN(t.title) AS movie_based_on_book
FROM postgres.company_name AS cn
INNER JOIN postgres.movie_companies AS mc ON cn.id = mc.company_id
INNER JOIN postgres.company_type AS ct ON mc.company_type_id = ct.id
INNER JOIN postgres.title AS t ON mc.movie_id = t.id
INNER JOIN postgres.movie_link AS ml ON t.id = ml.movie_id
INNER JOIN postgres.link_type AS lt ON ml.link_type_id = lt.id
INNER JOIN postgres.movie_keyword AS mk ON t.id = mk.movie_id
INNER JOIN postgres.keyword AS k ON mk.keyword_id = k.id
WHERE cn.country_code <> '[pl]'
  AND ct.kind <> 'production companies'
  AND ct.kind IS NOT NULL
  AND k.keyword IN ('sequel', 'revenge', 'based-on-novel')
  AND mc.note IS NOT NULL
  AND t.production_year > 1950;
