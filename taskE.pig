
access_log = LOAD '/project2/Input_Files/samp_AccessLogs.csv' USING PigStorage(',')
    AS (field1:chararray, userId:chararray, pageId:chararray, field4:chararray, field5:chararray);


grouped_users = GROUP access_log BY userId;


total_pages = FOREACH grouped_users GENERATE group AS userId, COUNT(access_log) AS TotalPages;


distinct_pages = FOREACH grouped_users {
    distinct_page_ids = DISTINCT access_log.pageId;
    GENERATE group AS userId, COUNT(distinct_page_ids) AS DistinctPages;
};


user_page_stats = JOIN total_pages BY userId, distinct_pages BY userId;


final_output = FOREACH user_page_stats GENERATE total_pages::userId AS userId,total_pages::TotalPages, distinct_pages::DistinctPages;
STORE final_output INTO '/project2/Output_Files/taskE_output' USING PigStorage(',');
