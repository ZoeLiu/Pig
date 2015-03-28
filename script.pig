register piggybank.jar;

DEFINE EXTRACT org.apache.pig.piggybank.evaluation.string.RegexExtractAll();

RAW_LOGS = LOAD 'input2' USING TextLoader as (line:chararray); 

LOGS_BASE = foreach RAW_LOGS generate FLATTEN ( EXTRACT (line, 
'^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] "(.+?)" (\\S+) (\\S+) "([^"]*)" "([^"]*)"') ) 
as ( 
remoteAddr:chararray, remoteLogname:chararray, user:chararray, time:chararray, request:chararray,status:int, bytes_string:chararray,referrer:chararray,browser:chararray); 

REFERRER_ONLY = FOREACH LOGS_BASE GENERATE referrer; 

FILTERED = FILTER REFERRER_ONLY BY referrer matches '.*bing.*' OR referrer matches '.*google.*'; 

SEARCH_TERMS = FOREACH FILTERED GENERATE FLATTEN(EXTRACT(referrer, '.*[&\\?]q=([^&]+).*')) as terms:chararray; 

SEARCH_TERMS_FILTERED = FILTER SEARCH_TERMS BY NOT $0 IS NULL; 

SEARCH_TERMS_COUNT = FOREACH (GROUP SEARCH_TERMS_FILTERED BY $0) GENERATE $0,COUNT($1) as num; 

SEARCH_TERMS_COUNT_SORTED = LIMIT(ORDER SEARCH_TERMS_COUNT BY num DESC) 50; 

STORE SEARCH_TERMS_COUNT_SORTED into 'output2';