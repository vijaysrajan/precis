
if [ $# -eq 0 ]
  then
       echo "No arguments supplied. Need a number which is the support threshold.";
       exit 1;
fi

#This is the best way to run the code

#java -Xmx6g -cp ../external_jars/commons-cli-1.2/commons-cli-1.2.jar: com.precis.aprioriOnAggregates.singleMachine.LaunchAprioriAnalyser -d dataFile -s schemaFile -t $1
#java -Xmx6g -cp "precis.jar:../external_jars/commons-cli-1.2/*" com.precis.aprioriOnAggregates.singleMachine.LaunchAprioriAnalyser -d dataFile -s schemaFile -t $1
java -Xmx2g  -jar "precis.jar" com.precis.aprioriOnAggregates.singleMachine.LaunchAprioriAnalyser -d $1 -s $2 -t $3 -m $4


