lines = LOAD 'w4q2.txt' USING TextLoader() AS (line:chararray);

words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) AS word;

grouped_words = GROUP words BY word;

word_counts = FOREACH grouped_words GENERATE group AS word, COUNT(words) AS count;

STORE word_counts INTO 'outputw4q2' USING PigStorage(',');
