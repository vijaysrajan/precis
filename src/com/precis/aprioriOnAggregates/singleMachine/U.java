package com.precis.aprioriOnAggregates.singleMachine;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;



class U {

   public static void cleanUpBaseFeedInBitSetFormatToRemoveRowsAsStageProgresses(int stage ) { 
      String openBrace   = "{" ;
      String smallerData = "[^,][^,]*,[^,][^,]*";
      String comma       = ",";
      String closeBrace  = "}" ;

      StringBuilder sb = new StringBuilder();
      sb.append(openBrace);
      sb.append(smallerData);
      if (1 <= (stage - 1)) {
          sb.append(comma);
      }   
      for (int i = 1 ; i < (stage - 1); i++) {
           sb.append(smallerData);
           if ( 1 < (stage - 1)) {
                sb.append(comma);  
           }
      }   
      if (1 <= (stage - 1)) {
           sb.append(smallerData);
      }   
      sb.append(closeBrace);

      String command = "cat " + Util.basicDataFileBitsetFormat  + " | " + " grep -ve  " + sb.toString() + " > " + Util.basicDataFileBitsetFormat + "_tmp; cp " + Util.basicDataFileBitsetFormat + "_tmp " + Util.basicDataFileBitsetFormat ;
      System.out.println(command);
//      Process p = Runtime.getRuntime().exec(new String[]{"bash","-c",command});
//      int ret_code = -1; 
//      try {
//           ret_code = p.waitFor();
//      } catch (Exception e) {
//           e.printStackTrace();
//      }   
   }   

   public static void main(String[] args) { 

     cleanUpBaseFeedInBitSetFormatToRemoveRowsAsStageProgresses(1);
     cleanUpBaseFeedInBitSetFormatToRemoveRowsAsStageProgresses(2);
     cleanUpBaseFeedInBitSetFormatToRemoveRowsAsStageProgresses(3);
     cleanUpBaseFeedInBitSetFormatToRemoveRowsAsStageProgresses(4);
     cleanUpBaseFeedInBitSetFormatToRemoveRowsAsStageProgresses(5);
     
   }


}
