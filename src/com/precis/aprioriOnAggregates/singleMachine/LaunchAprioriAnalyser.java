package com.precis.aprioriOnAggregates.singleMachine;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;
import java.lang.StringBuilder;
import java.io.FileNotFoundException;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.BasicParser;


//uses local classes DataLoader 

public class LaunchAprioriAnalyser {

        private static String dataFile   = "";
        private static String schemaFile = "";
	    private static double threshold = 0;
        private static String usage      ="Usage: java -cp ../external_jars/commons-cli-1.2/commons-cli-1.2.jar: com.precis.aprioriOnAggregates.singleMachine.LaunchAprioriAnalyser -d DataFileName -s schemaFileName -t supportThreshold";
        private static Integer maxNumberOfStages;

        private static String fileMissingError() {
            StringBuilder sb = new StringBuilder();
            sb.append("Data file(");
            sb.append(dataFile);
            sb.append(") or Schema file(");
            sb.append(schemaFile);
            sb.append(") is not valid"); 
            return sb.toString();    
        }

        private static void processArguments(String [] args) {
                Options o = new Options();
                o.addOption("d", true, "data filename");
                o.addOption("s", true, "schema for data");
                o.addOption("t", true, "support threshold");
                o.addOption("m",true, "maximum number of stages");
                Parser p = new BasicParser();
                try {
                       // parse the command line arguments
                       CommandLine line = p.parse( o, args );
                       if (!line.hasOption("d") || !line.hasOption( "s") || !line.hasOption( "t") ) {
                           throw new Exception();
                       }
                       dataFile   = line.getOptionValue( "d" );
                       schemaFile = line.getOptionValue( "s" );
		               threshold = Double.parseDouble(line.getOptionValue( "t" ));
                    if (!line.hasOption("m")) {
                        maxNumberOfStages = 1000000000;
                    } else {
                        maxNumberOfStages = Integer.parseInt(line.getOptionValue("m"));
                    }
                 } catch( Exception exp ) {
                       System.out.println(usage);
                       System.exit(1);
                 }
        }

        private static void fileExistsCheck() {
                File fDataFile = new File(dataFile);
                File fSchemaFile = new File(schemaFile);
                if ((!fDataFile.exists()) && (!fSchemaFile.exists())) {
                    System.out.println(fileMissingError());
                    System.exit(2);
                }
        }

        private static BufferedReader doDataLoading()  throws FileNotFoundException {
                DataLoader dl = new DataLoader();
                return dl.loadData(dataFile);
        }

        private static void startToFeed (BufferedReader br) throws IOException {
                Feeder fd = new Feeder(schemaFile,threshold,maxNumberOfStages);
                fd.startToFeed(br);     
        }

        public static void main (String [] args) throws IOException,FileNotFoundException {
                processArguments(args);
                fileExistsCheck();
                startToFeed(doDataLoading());
        }
}

