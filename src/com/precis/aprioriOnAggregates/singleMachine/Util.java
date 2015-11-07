package com.precis.aprioriOnAggregates.singleMachine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.FileReader;
import java.lang.StringBuilder;
import java.util.Arrays;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.FileReader;
import java.lang.StringBuilder;
import java.util.Arrays;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.nio.charset.Charset;


public class Util {
      public static final String separatorBetweenDimAndVal  = "";
      public static final String separatorBetweenFields     = "";
      public static final String mapToSS_Stage1             ="MapToSS.tmp";
      public static final String redOp_Stage1               ="1_stage_string_output.tmp";
      public static final String SSToRed_Stage1             ="SSToRed.tmp";
      public static final String dim_mapping                ="dim_mapping.tmp";
      public static final String dim_val_mapping            ="dim_val_mapping.tmp";
      public static final String schemaFile                 ="schemaFile";
      public static final String basicDataFile              ="dataFile";
      public static final String basicDataFileBitsetFormat  ="bitsetDataFile";
      public static final String redopStage1Bitset          ="bitsetRedOp1.tmp";

      public static void writeArray (String filename, String[]x) throws IOException{
           BufferedWriter outputWriter = null;
           outputWriter = new BufferedWriter(new FileWriter(filename));
           for (int i = 0; i < x.length; i++) {
               outputWriter.write(x[i]+"");
               // Or: 
               //outputWriter.write(Integer.toString(x[i]);
               outputWriter.newLine();
           }   
           outputWriter.flush();  
           outputWriter.close();  
      }

      public static void sortFileAIntoFileB (String a, String b) throws IOException {
           List<String> unsortedList = null;
           String [] unsortedArray = null;
           unsortedList = Files.readAllLines(Paths.get(a), Charset.defaultCharset()); 
           unsortedArray = new String[unsortedList.size()];
           unsortedArray = unsortedList.toArray(unsortedArray);
           Arrays.sort(unsortedArray);  //now unsortedArray is sorted
           Util.writeArray(b, unsortedArray);
      }


}
