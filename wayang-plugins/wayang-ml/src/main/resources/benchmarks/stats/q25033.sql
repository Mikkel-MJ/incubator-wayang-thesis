select count(*)
from postgres.comments as c,
     postgres.posts as p,
     postgres.postlinks as pl,
     postgres.users as u
where p.id = c.postid
  and p.id = pl.relatedpostid
  and p.owneruserid = u.id
  and c.creationdate >= timestamp '2010-07-21 11:05:37'
  and c.creationdate <= timestamp '2014-08-25 17:59:25'
  and u.upvotes >= 0
  and u.creationdate >= timestamp '2010-08-21 21:27:38';
