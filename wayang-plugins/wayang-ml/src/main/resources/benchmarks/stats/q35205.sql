select count(*)
from postgres.comments as c,
     postgres.posts as p,
     postgres.postlinks as pl,
     postgres.posthistory as ph,
     postgres.users as u
where pl.relatedpostid = p.id
  and u.id = c.userid
  and c.postid = p.id
  and ph.postid = p.id
  and c.creationdate >= timestamp '2010-07-11 12:25:05'
  and c.creationdate <= timestamp '2014-09-11 13:43:09'
  and p.commentcount >= 0
  and p.commentcount <= 14
  and pl.linktypeid = 1
  and ph.creationdate >= timestamp '2010-08-06 03:14:53'
  and u.reputation >= 1
  and u.reputation <= 491
  and u.downvotes >= 0
  and u.downvotes <= 0;
