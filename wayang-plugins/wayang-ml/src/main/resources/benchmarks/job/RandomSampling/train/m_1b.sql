SELECT 
    MIN(mc.note) AS production_note,
    MIN(t.title) AS movie_title,
    MIN(t.production_year) AS movie_year
FROM 
    postgres.company_type AS ct
    INNER JOIN postgres.movie_companies AS mc ON ct.id = mc.company_type_id
    INNER JOIN postgres.movie_info_idx AS mi_idx ON mc.movie_id = mi_idx.movie_id
    INNER JOIN postgres.title AS t ON t.id = mc.movie_id 
    INNER JOIN postgres.info_type AS it ON it.id = mi_idx.info_type_id
WHERE ct.kind = 'production companies'
    AND it.info = 'bottom 10 rank'
    AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
    AND t.production_year BETWEEN 2005 AND 2010
    AND t.id = mi_idx.movie_id;
