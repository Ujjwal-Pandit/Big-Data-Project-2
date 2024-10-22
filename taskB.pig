access_log = LOAD '/project2/Input_Files/samp_AccessLogs.csv' USING PigStorage(',')
    AS (pageid:chararray, field2:chararray, field3:chararray, field4:chararray, degree:chararray);

page_counts = GROUP access_log BY pageid;
page_access_counts = FOREACH page_counts GENERATE group AS pageid, COUNT(access_log) AS access_count;

top_10_pages = ORDER page_access_counts BY access_count DESC;
top_10_pages = LIMIT top_10_pages 10;

linkbook_pages = LOAD '/project2/Input_Files/samp_LinkBookPage.csv' USING PigStorage(',')
 AS (id:chararray, nickname:chararray, occupation:chararray, field4:chararray, field5:chararray);

result = JOIN top_10_pages BY pageid, linkbook_pages BY id;

final_result = FOREACH result GENERATE linkbook_pages::id AS Id, linkbook_pages::nickname AS NickName, linkbook_pages::occupation AS Occupation;

STORE final_result INTO '/project2/Output_Files/taskB_output' USING PigStorage(',');