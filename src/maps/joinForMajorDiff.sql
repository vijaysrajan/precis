create table data727(key, value number);
.import test_727/joinDataMeanOnloadForSignificantSets data727

create table data803(key, value number);
.import  test_803/joinDataMeanOnloadForSignificantSets data803


.output  MainDifferences
select a.key,  b.value - a.value from data803 a, data727 b where a.key = b.key order by 2 desc;


