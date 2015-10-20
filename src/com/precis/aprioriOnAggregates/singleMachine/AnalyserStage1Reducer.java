package com.precis.aprioriOnAggregates.singleMachine;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.io.FileNotFoundException;
import java.lang.StringBuilder;
import java.math.BigDecimal;
import java.text.DecimalFormat;


public class AnalyserStage1Reducer implements FeederToReducerIface  {
	BufferedWriter bw = null;
	String currentKey = null;
	boolean keyChange = false;
	double sum = 0;
        StringBuilder sb = new StringBuilder();
	String redOpFName = null;
	double threshold = 0;
        DecimalFormat df = null;

	public AnalyserStage1Reducer(double t, String redOpFileName) throws IOException, FileNotFoundException{
		redOpFName = redOpFileName;	
		bw = new BufferedWriter (new FileWriter(redOpFName));
		threshold = t;

                df = new DecimalFormat("#");
                df.setMaximumFractionDigits(1);
        }
	public void evaluate (String rec){

		String [] _parts = rec.split (Util.separatorBetweenFields);
		String [] parts  = _parts[0].split(Util.separatorBetweenDimAndVal);

                try {
                        sb = new StringBuilder();
			sb.append(parts[0]);
                        sb.append(Util.separatorBetweenDimAndVal);
                        sb.append(parts[1]);

			if (currentKey == null) {
				keyChange = false;
				currentKey = sb.toString();
				sum = Double.parseDouble(_parts[1]);
			} else if (currentKey.compareTo(sb.toString())==0) {
				keyChange = false;
				sum += Double.parseDouble(_parts[1]);
			} else {
				keyChange = true;
				String newKey  = sb.toString();
				if (sum > threshold) {
			        	StringBuilder sb2 = new StringBuilder();
					sb2.append(currentKey);
                        		sb2.append(Util.separatorBetweenFields);
					sb2.append(df.format(sum));
                                	bw.write(sb2.toString(),0,sb2.length());
                                	bw.newLine();
                                	bw.flush();
				}
				sum = Double.parseDouble(_parts[1]);
				currentKey = newKey;
			}
                } catch (Exception E) {
			E.printStackTrace();
                        System.out.println("Mapper.evaluate Failed");
                        System.exit(3);
                }
	}
	public void done() {
                try {
			if (sum > threshold) {
				sb = new StringBuilder();	
				sb.append(currentKey);
                        	sb.append(Util.separatorBetweenFields);
				sb.append(df.format(sum));
                        	bw.write(sb.toString(),0,sb.length());
				bw.flush();
			}
                        bw.close();
                } catch (Exception e) {
                        System.out.println("Mapper.done Failed");
                        System.exit(4);
                }
        }

}

