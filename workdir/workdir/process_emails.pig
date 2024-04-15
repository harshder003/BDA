-- Load the data
emails = LOAD '/workdir/emails.txt' USING PigStorage('\t') AS (user_id:chararray, from_email:chararray, to_emails:chararray);

-- Transform the 'to_emails' field to remove parentheses and split by comma
emails_clean = FOREACH emails GENERATE 
    user_id, 
    from_email, 
    FLATTEN(STRSPLIT(REPLACE(to_emails, '\\(|\\)', ''), ',')) AS to_email;

-- Group data by 'user_id' and collect all recipients
grouped_emails = GROUP emails_clean BY user_id;
final_emails = FOREACH grouped_emails GENERATE 
    group AS user_id, 
    emails_clean.from_email AS from_email, 
    emails_clean.to_email AS to_emails;

-- Store the result
STORE final_emails INTO 'output_emails' USING PigStorage(',');
