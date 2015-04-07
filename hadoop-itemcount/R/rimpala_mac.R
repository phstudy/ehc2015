#!/usr/bin/Rscript

# http://blog.cloudera.com/blog/2013/12/how-to-do-statistical-analysis-with-impala-and-r/

Sys.setenv(JAVA_HOME="/System/Library/Java/JavaVirtualMachines/1.6.0.jdk/Contents/Home") 

options(java.parameters = "-Xmx1000m")

library(RImpala)

rimpala.init(libs="./lib")

rimpala.connect(IP="em2", port="21050")

rimpala.usedatabase("default")

rimpala.query("select row_number() over (order by count desc) as rank, item from ( select count(*) as count, item from d01 group by item order by count desc limit 20 ) as tbl");

rimpala.close()
