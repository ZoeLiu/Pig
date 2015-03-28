register /usr/lib/pig/piggybank.jar;

RAW_COUNTS = LOAD 'input4' USING PigStorage('\t') AS
		(cited:chararray,count:int);
        
grouped = GROUP RAW_COUNTS by count;

COUNTS_HIST = foreach grouped GENERATE 
                              group AS cite_count,
                              COUNT(RAW_COUNTS.cited) AS num_cite_count;

HIST_DESC = ORDER COUNTS_HIST by cite_count DESC;
HIST_ASC  = ORDER COUNTS_HIST by cite_count;

STORE HIST_DESC into 'output4_histDESC';
STORE HIST_ASC into 'output4_histASC';


