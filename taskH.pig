associates = LOAD '/project2/Input_Files/samp_Associates.csv' USING PigStorage(',')
              AS (id1:chararray, id2:chararray);

accessLogs = LOAD '/project2/Input_Files/samp_AccessLogs.csv' USING PigStorage(',')
              AS (field1:chararray, id:chararray, field3:chararray, field4:chararray, time:chararray);

linkBookPage = LOAD '/project2/Input_Files/samp_LinkBookPage.csv' USING PigStorage(',')
                AS (id:chararray, nickname:chararray);

-- Step 1: Filter users who accessed their friends' LinkBookPage
accessedFriends = JOIN associates BY id2, accessLogs BY id;

-- Step 2: Get the distinct list of id1 who accessed their friends' LinkBookPage
accessedIds = FOREACH accessedFriends GENERATE associates::id1 AS id1;
distinctAccessed = DISTINCT accessedIds;

-- Step 3: Identify users (id1) who never accessed their friends' LinkBookPage
-- Perform a LEFT OUTER JOIN and then filter the null entries (those who didn't access)
allUsers = JOIN associates BY id1 LEFT OUTER, distinctAccessed BY id1;
nonAccessors = FILTER allUsers BY distinctAccessed::id1 IS NULL;

-- Step 4: Join the non-accessors with LinkBookPage to get their nicknames
finalJoin = JOIN nonAccessors BY associates::id1, linkBookPage BY id;

finalOutput = FOREACH finalJoin GENERATE 
                nonAccessors::associates::id1 AS id, 
                linkBookPage::nickname AS nickname;

STORE finalOutput INTO '/project2/Output_Files/taskH_output' USING PigStorage(',');

