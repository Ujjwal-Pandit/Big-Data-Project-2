linkbook_pages = LOAD '/project2/Input_Files/samp_LinkBookPage.csv' USING PigStorage(',')
    AS (id:chararray, nickname:chararray, field3:chararray, field4:chararray, field5:chararray);

associates = LOAD '/project2/Input_Files/samp_Associates.csv' USING PigStorage(',')
    AS (field1:chararray, id1:chararray, id2:chararray, field4:chararray, field5:chararray);

-- Create a happiness factor of 1 for each association in the Associates file
happiness1 = FOREACH associates GENERATE id1 AS id, 1 AS happiness_factor;
happiness2 = FOREACH associates GENERATE id2 AS id, 1 AS happiness_factor;


-- Combine both happiness mappings
all_happiness = UNION happiness1, happiness2;

-- Group by id to calculate the total happiness factor for each user
grouped_happiness = GROUP all_happiness BY id;
user_happiness = FOREACH grouped_happiness GENERATE group AS id, SUM(all_happiness.happiness_factor) AS happiness_factor;

-- Join happiness factors with LinkBookPage to get the nickname
result = JOIN user_happiness BY id, linkbook_pages BY id;

-- Select the final output: Nickname and Happiness Factor
final_result = FOREACH result GENERATE linkbook_pages::nickname AS NickName, user_happiness::happiness_factor AS HappinessFactor;

STORE final_result INTO '/project2/Output_Files/taskD_output' USING PigStorage(',');