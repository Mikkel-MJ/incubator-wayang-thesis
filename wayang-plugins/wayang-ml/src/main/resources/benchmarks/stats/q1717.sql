select count(*)
from postgres.postlinks as pl,
     postgres.posts as p,
     postgres.users as u,
     postgres.badges as b
where p.id = pl.relatedpostid
  and u.id = p.owneruserid
  and u.id = b.userid
  and pl.creationdate <= timestamp '2014-08-17 01:23:50'
  and p.score >= -1
  and p.score <= 10
  and p.answercount <= 5
  and p.commentcount = 2
  and p.favoritecount >= 0
  and p.favoritecount <= 6
  and u.views <= 33
  and u.downvotes >= 0;
