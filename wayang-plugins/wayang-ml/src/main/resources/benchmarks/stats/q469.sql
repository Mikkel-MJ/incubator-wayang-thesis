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
  and c.creationdate >= timestamp '2010-08-02 20:27:48'
  and c.creationdate <= timestamp '2014-09-10 16:09:23'
  and p.posttypeid = 1
  and p.score = 4
  and p.viewcount <= 4937
  and pl.creationdate >= timestamp '2011-11-03 05:09:35'
  and ph.posthistorytypeid = 1
  and u.reputation <= 270
  and u.views >= 0
  and u.views <= 51
  and u.downvotes = 0;
