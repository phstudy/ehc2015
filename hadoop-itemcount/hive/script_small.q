select count(*) as count, item from d01 group by item order by count desc limit 20;

