


create table mainDiff(key, value number);
.import MainDifferences  mainDiff

create table comparison(key, value number);
.import comparisonOfSignificantSetsUsingBeaconCountingEquiJoin  comparison


.output final 
select a.key, ( a.value * b.value) from comparison a, mainDiff  b where a.key = b.key and a.value > 0 and b.balue > 0 order by 2 desc;


