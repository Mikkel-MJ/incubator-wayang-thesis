select count(*) from postgres.comments as c, postgres.votes as v where c.userid = v.userid and c.score=0;
