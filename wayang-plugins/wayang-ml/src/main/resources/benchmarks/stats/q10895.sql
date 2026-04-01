select count(*)
from postgres.posts as p,
     postgres.postlinks as pl,
     postgres.users as u
where p.id = pl.postid
  and p.owneruserid = u.id
  and p.commentcount <= 17
  and u.creationdate <= timestamp '2014-09-12 07:12:16';
