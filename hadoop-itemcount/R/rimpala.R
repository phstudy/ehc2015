#!/usr/bin/Rscript

# http://blog.cloudera.com/blog/2013/12/how-to-do-statistical-analysis-with-impala-and-r/

options(java.parameters = "-Xmx1000m")

library(RImpala)

rimpala.init("/usr/lib/impala/lib")

rimpala.connect(IP="localhost", port="21050")

rimpala.usedatabase("default")

rimpala.query("select row_number() over (order by count desc) as rank, item from ( select count(*) as count, item from d01_500 group by item order by count desc limit 20 ) as tbl");

rimpala.close()
