
create table d727Count(key, value number);
.import ./test_727/sigSets_20140727_for_count d727Count

create table d803Count(key, value number);
.import ./test_803/sigSets_20140803_for_count d803Count


.output comparisonOfSignificantSetsUsingBeaconCountingEquiJoin
select a.key, ( a.value - b.value) from d727Count a, d803Count b where a.key = b.key;


