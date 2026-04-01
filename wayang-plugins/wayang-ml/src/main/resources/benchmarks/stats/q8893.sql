select count(*)
from postgres.comments as c,
     postgres.posts as p,
     postgres.postlinks as pl,
     postgres.votes as v,
     postgres.badges as b,
     postgres.users as u
where p.id = c.postid
  and p.id = pl.relatedpostid
  and p.id = v.postid
  and u.id = p.owneruserid
  and u.id = b.userid
  and c.score = 0
  and p.answercount >= 0
  and p.answercount <= 4
  and p.creationdate <= timestamp '2014-09-12 15:56:19'
  and pl.linktypeid = 1
  and pl.creationdate >= timestamp '2011-03-07 16:05:24'
  and v.bountyamount <= 100
  and v.creationdate >= timestamp '2009-02-03 00:00:00'
  and v.creationdate <= timestamp '2014-09-11 00:00:00'
  and u.views <= 160
  and u.creationdate >= timestamp '2010-07-27 12:58:30'
  and u.creationdate <= timestamp '2014-07-12 20:08:07';
