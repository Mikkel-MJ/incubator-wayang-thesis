SELECT MIN(an.name) AS alternative_name,
       MIN(chn.name) AS voiced_char_name,
       MIN(n.name) AS voicing_actress,
       MIN(t.title) AS american_movie
FROM postgres.aka_name AS an
INNER JOIN postgres.name AS n ON an.person_id = n.id
INNER JOIN postgres.cast_info AS ci ON n.id = ci.person_id
INNER JOIN postgres.title AS t ON ci.movie_id = t.id
INNER JOIN postgres.movie_companies AS mc ON t.id = mc.movie_id
INNER JOIN postgres.company_name AS cn ON mc.company_id = cn.id
INNER JOIN postgres.role_type AS rt ON ci.role_id = rt.id
INNER JOIN postgres.char_name AS chn ON chn.id = ci.person_role_id
WHERE ci.note IN ('(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)')
  AND cn.country_code = '[us]'
  AND n.gender = 'f'
  AND rt.role = 'actress';
