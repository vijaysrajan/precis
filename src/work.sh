cp maps/data_ending_803/dataFile_20140803_onload_time ./dataFile
./clear.sh ; ./run.sh  7005695;  cat *_stage_string_output.tmp > sigSets_20140803_for_onload_time ; ./clear.sh
cp maps/data_ending_803/dataFile_20140803_count ./dataFile
./clear.sh ; ./run.sh  1261 ;  cat *_stage_string_output.tmp > sigSets_20140803_for_count ; ./clear.sh ; 


cat sigSets_20140803_for_onload_time | sed "s/\([0-9][0-9]*.[0-9]*\)$/|\1/" > 20140803_for_onload_time
cat sigSets_20140803_for_count | sed "s/\([0-9][0-9]*.[0-9]*\)$/|\1/" > 20140803_for_count

mv 20140803_for_count sigSets_20140803_for_count
mv 20140803_for_onload_time sigSets_20140803_for_onload_time

mv sigSets_20140803_for_onload_time maps/test_803/
mv sigSets_20140803_for_count       maps/test_803/


cp maps/data_ending_727/dataFile_20140727_onload_time ./dataFile
./clear.sh ; ./run.sh  7005695;  cat *_stage_string_output.tmp > sigSets_20140727_for_onload_time ; ./clear.sh
cp maps/data_ending_727/dataFile_20140727_count ./dataFile
./clear.sh ; ./run.sh  1261 ;  cat *_stage_string_output.tmp > sigSets_20140727_for_count ; ./clear.sh ; 


cat sigSets_20140727_for_onload_time | sed "s/\([0-9][0-9]*.[0-9]*\)$/|\1/" > 20140727_for_onload_time
cat sigSets_20140727_for_count | sed "s/\([0-9][0-9]*.[0-9]*\)$/|\1/" > 20140727_for_count

mv 20140727_for_count sigSets_20140727_for_count
mv 20140727_for_onload_time sigSets_20140727_for_onload_time

mv sigSets_20140727_for_onload_time maps/test_727/
mv sigSets_20140727_for_count       maps/test_727/

