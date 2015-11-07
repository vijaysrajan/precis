package com.precis.aprioriOnAggregates.singleMachine;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.io.FileNotFoundException;
import java.lang.StringBuilder;


public class AnalyserStage1Mapper implements FeederToMapperIface {
        ArrayList<String> dims= new ArrayList<String>();
        ArrayList<String> mtrx= new ArrayList<String>();
	BufferedWriter bw = null; //new BufferedWriter (new FileWriter("MapToSS"));

	public AnalyserStage1Mapper(String schemaFile, String fileNameFOrMapToSS) throws IOException, FileNotFoundException{
		BufferedReader br = new BufferedReader(new FileReader(schemaFile));
		bw = new BufferedWriter (new FileWriter(fileNameFOrMapToSS));
		String line = null;
		while ( (line = br.readLine()) != null) {
			String [] parts = line.split(Util.separatorBetweenFields);
                        if (parts[1].compareTo("d") == 0) {
			   dims.add(parts[0]);
                        } else {
			   mtrx.add(parts[0]);
                        }
		}
	}

	public void evaluate (String rec) {
		String [] parts = rec.split (Util.separatorBetweenFields);
		StringBuilder sb = new StringBuilder();
		try {
			for (int i = 0; i < dims.size(); i++) {
				//System.out.println ("length : " +   parts.length);
				if (parts[i].equalsIgnoreCase("null") || parts[i].equalsIgnoreCase("unknown") || parts[i].equalsIgnoreCase("na") ) {
					continue;
				}
				sb.append(dims.get(i));
				sb.append(Util.separatorBetweenDimAndVal);
				sb.append(parts[i]);
				sb.append(Util.separatorBetweenFields);
				sb.append(parts[parts.length -1]);
				bw.write(sb.toString(),0,sb.length());
				bw.newLine();
				sb = new StringBuilder();
				
				bw.flush();
			}
		} catch (Exception E) { 
			E.printStackTrace();
			System.out.println("Mapper.evaluate Failed");
			System.exit(3);
		}
		

        }

	public void done() {
		try {
			bw.flush();
		} catch (Exception e) {
			System.out.println("Mapper.done Failed");
			System.exit(4);
		}
	}

}
