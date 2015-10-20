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
}
