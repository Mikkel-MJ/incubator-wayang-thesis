select count(*)
from postgres.comments as c,
     postgres.postlinks as pl,
     postgres.posts as p,
     postgres.users as u,
     postgres.badges as b
where p.id = pl.relatedpostid
  and p.id = c.postid
  and u.id = b.userid
  and u.id = p.owneruserid
  and pl.linktypeid = 1
  and pl.creationdate >= timestamp '2011-04-12 15:23:59'
  and p.score = 1
  and p.viewcount >= 0
  and p.favoritecount >= 0
  and u.creationdate >= timestamp '2011-02-08 18:11:37';
