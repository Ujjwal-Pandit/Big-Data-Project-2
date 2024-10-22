users = LOAD '/project2/Input_Files/samp_LinkBookPage.csv'
    USING PigStorage(',')
    AS (userID:chararray, nickname:chararray);

relationships = LOAD '/project2/Input_Files/samp_Associates.csv'
    USING PigStorage(',')
    AS (associateID:int, userA:chararray, userB:chararray);

flatUserA = FOREACH relationships GENERATE userA AS userID, 1 AS relCount;
flatUserB = FOREACH relationships GENERATE userB AS userID, 1 AS relCount;

allRelationships = UNION flatUserA, flatUserB;

groupedUsers = GROUP allRelationships BY userID;
userRelCounts = FOREACH groupedUsers GENERATE group AS userID, COUNT(allRelationships) AS relCount;

totals = FOREACH (GROUP userRelCounts ALL) GENERATE
    COUNT(userRelCounts) AS totalUsers,
    SUM(userRelCounts.relCount) AS totalRels;

avgRels = FOREACH totals GENERATE (double)totalRels / totalUsers AS avgRelCount;

popularUsers = FILTER userRelCounts BY relCount > avgRels.avgRelCount;

joinedUsers = JOIN popularUsers BY userID, users BY userID;

output = FOREACH joinedUsers GENERATE users::nickname, popularUsers::relCount;

STORE output INTO '/projejct2/Output_Files/taskF_output'
    USING PigStorage(',');