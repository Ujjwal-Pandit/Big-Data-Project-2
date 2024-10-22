linkbook_pages = LOAD '/project2/Input_Files/samp_LinkBookPage.csv' USING PigStorage(',')
    AS (id:chararray, nickname:chararray, occupation:chararray, field4:chararray, highestEdu:chararray);

ms_students = FILTER linkbook_pages BY highestEdu == 'MS';

result = FOREACH ms_students GENERATE nickname, occupation;

STORE result INTO '/project2/Output_Files/taskC_output' USING PigStorage(',');