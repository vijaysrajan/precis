package com.precis.aprioriOnAggregates.singleMachine;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;

public class DataLoader {

	public BufferedReader loadData (String fname) throws FileNotFoundException {

               return new BufferedReader(new FileReader(fname));
               /*
                 // BufferedReader  dataFile = null;
                 // String sCurrentLine;
                 //
                 // br = new BufferedReader(new FileReader(fname);
                 // while (sCurrentLine = br.readLine()) != null)
                 // {
                 //    (sCurrentLine);
                 // }
               */
         }

}
