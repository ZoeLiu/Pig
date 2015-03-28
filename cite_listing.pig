register /usr/lib/pig/piggybank.jar;

DEFINE EXTRACT org.apache.pig.piggybank.evaluation.string.Split();

RAW_CITES = LOAD 'input2' USING TextLoader as (line:chararray);

CITES_BASE = foreach RAW_CITES generate FLATTEN (EXTRACT(line,',')) as (
                           citing:chararray, cited:chararray);

CITE_CITING = foreach CITES_BASE generate cited, citing;

grouped = GROUP CITE_CITING by $0;

CITE_GROUP = foreach grouped GENERATE
                           group AS cited, 
                           CITE_CITING.citing as clist;

STORE CITE_GROUP into 'output2';