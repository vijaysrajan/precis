


create table beacon(key, value number);
.import ./sigSets_20140803_for_count beacon

create table onload(key, value number);
.import sigSets_20140803_for_onload_time onload


.output joinDataMeanOnloadForSigNificantSets
select a.key, ( 1.0 * b.value / a.value) from beacon a, onload b where a.key = b.key;


