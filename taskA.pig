data = LOAD '/project2/Input_Files/samp_LinkBookPage.csv' USING PigStorage(',')
    AS (field1:chararray, field2:chararray, field3:chararray, field4:chararray, degree:chararray);

grouped_data = GROUP data BY degree;

degree_count = FOREACH grouped_data GENERATE group AS degree, COUNT(data) AS count;

STORE degree_count INTO '/project2/Output_Files/taskA_output' USING PigStorage(',');
