select count(*)
from postgres.votes as v,
     postgres.posts as p,
     postgres.users as u
where v.postid = p.id
  and v.userid = u.id
  and v.creationdate <= timestamp '2014-09-12 00:00:00'
  and p.score >= -1
  and p.creationdate >= timestamp '2010-10-21 13:21:24'
  and p.creationdate <= timestamp '2014-09-09 15:12:22'
  and u.upvotes >= 0
  and u.creationdate >= timestamp '2010-07-27 17:15:57'
  and u.creationdate <= timestamp '2014-09-03 12:47:42';
