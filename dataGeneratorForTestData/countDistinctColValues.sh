for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15
do 
     cat dataFile | awk -F"" -v tmp=${i} '{print $tmp}' | sort -u | wc -l
done;

