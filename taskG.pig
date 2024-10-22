accessLogs = LOAD '/project2/Input_Files/samp_AccessLogs.csv' USING PigStorage(',') 
              AS (field1:chararray, id:chararray, field3:chararray, field4:chararray, time:int); 

-- Filter users with access time greater than 129600 minutes (90 days)
outdatedUsers = FILTER accessLogs BY (time > 129600);  -- Corrected ToINT to ToInt

-- Group by ID to prepare for distinct IDs
groupedOutdatedUsers = GROUP outdatedUsers BY id;

distinct_ids = FOREACH groupedOutdatedUsers GENERATE group AS id;

linkBookPage = LOAD '/project2/Input_Files/samp_LinkBookPage.csv' USING PigStorage(',') 
                AS (id:chararray, nickname:chararray);

-- Join distinct IDs with LinkBookPage to get nicknames
finalJoin = JOIN distinct_ids BY id, linkBookPage BY id;

-- Select the final output (ID and Nickname)
finalOutput = FOREACH finalJoin GENERATE 
                distinct_ids::id AS id, 
                linkBookPage::nickname AS nickname;

STORE finalOutput INTO '/project2/Output_Files/taskG_output' USING PigStorage(',');

