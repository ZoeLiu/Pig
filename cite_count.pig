register /usr/lib/pig/piggybank.jar;


RAW_LIST = LOAD 'input3' USING PigStorage('\t') as (
           cited:chararray,citingbag:bag{T:tuple(citing:chararray)});

LIST_COUNT = FOREACH RAW_LIST GENERATE $0, COUNT($1); 

LIST_COUNT_ORDER = ORDER LIST_COUNT by $1 DESC;


STORE LIST_COUNT_ORDER into 'output3';