select count(*) from postgres.comments as c, postgres.posts as p, postgres.posthistory as ph where p.id = c.postid and p.id = ph.postid and p.commentcount>=0 and p.commentcount<=25;
