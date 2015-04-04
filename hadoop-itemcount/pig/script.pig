D01 = LOAD '/user/root/D01.csv' using PigStorage(',') AS 
(
	date_time:chararray, 
	id:chararray, 
	age:chararray, 
	area:chararray, 
	item_class:chararray, 
	item:chararray, 
	num:chararray, 
	cost:chararray, 
	price:chararray 
);

/* DESCRIBE D01; */ 

D01_GROUP = GROUP D01 BY item;
D01_COUNT = FOREACH D01_GROUP GENERATE COUNT(D01) as count, group as item;
D01_ORDER = ORDER D01_COUNT BY count DESC;
D01_LIMIT = LIMIT D01_ORDER 20;
D01_RANK = RANK D01_LIMIT BY count DESC;
DESCRIBE D01_RANK;
D01_RST = FOREACH D01_RANK GENERATE $0, item;


DUMP D01_RST; 
STORE D01_RST INTO '/user/root/out/pig' using PigStorage('\t','-schema');
