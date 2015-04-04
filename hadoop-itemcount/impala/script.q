select row_number() over (order by count desc) as rank, item from ( select count(*) as count, item from d01_500 group by item order by count desc limit 20 ) as tbl;
