A = LOAD './044/w41.txt' USING PigStorage(',') AS (name:chararray,value:int);

B = FILTER A BY value > 10;

STORE B INTO 'output_w41' USING PigStorage(',');
