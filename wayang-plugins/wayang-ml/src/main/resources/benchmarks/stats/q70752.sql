select count(*) from postgres.posts as p, postgres.tags as t, postgres.votes as v where p.id = t.excerptpostid and p.owneruserid = v.userid and p.creationdate>=timestamp '2010-07-20 02:01:05';
