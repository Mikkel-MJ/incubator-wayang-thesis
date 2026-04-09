select count(*)
from postgres.comments as c,
     postgres.posts as p,
     postgres.postlinks as pl,
     postgres.posthistory as ph,
     postgres.votes as v,
     postgres.users as u
where p.id = pl.postid
  and p.id = ph.postid
  and p.id = c.postid
  and u.id = c.userid
  and u.id = v.userid
  and c.score = 0
  and c.creationdate >= timestamp '2010-07-20 06:26:28'
  and c.creationdate <= timestamp '2014-09-11 18:45:09'
  and p.posttypeid = 1
  and p.favoritecount >= 0
  and p.favoritecount <= 2
  and ph.posthistorytypeid = 5
  and u.downvotes <= 0
  and u.upvotes >= 0
  and u.creationdate >= timestamp '2010-09-18 01:58:41';
