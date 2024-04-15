A = LOAD '/test_dir/w4q1.txt' USING PigStorage(',') AS (name:chararray, age:int);

B = FILTER A BY age > 30;

STORE B INTO 'output_w4q1' USING PigStorage(',');
