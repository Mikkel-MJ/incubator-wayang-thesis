select count(*)
from postgres.postlinks as pl,
     postgres.posts as p,
     postgres.users as u,
     postgres.badges as b
where p.id = pl.relatedpostid
  and u.id = p.owneruserid
  and u.id = b.userid
  and pl.linktypeid = 1
  and p.score >= -1
  and p.commentcount <= 8
  and p.creationdate >= timestamp '2010-07-21 12:30:43'
  and p.creationdate <= timestamp '2014-09-07 01:11:03'
  and u.views <= 40
  and u.creationdate >= timestamp '2010-07-26 19:11:25'
  and u.creationdate <= timestamp '2014-09-11 22:26:42';
