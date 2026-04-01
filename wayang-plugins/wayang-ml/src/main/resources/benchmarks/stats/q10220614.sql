select count(*) from postgres.comments as c, postgres.badges as b where c.userid = b.userid and c.score=0 and b."date" <= timestamp '2014-09-11 14:33:06';
