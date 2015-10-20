package com.precis.aprioriOnAggregates.singleMachine;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class Partition {
    Map<ComparableBitSet,String> hashmap;
	int stageMinus2;
	BufferedReader reader;
	String line="";
	String key;
	String prevKey;
	int partitionCount;
	int noLines;
    StringFeedToBitsetFeedConverter converter;
	
	public Partition(String lastStageFilename, int stage, String dimMappingFile, String dimValMappingFile) throws Exception {
		this.partitionCount = 0;
		stageMinus2 = stage -2;
        lastStageFilename = System.getProperty("user.dir")+"/"+lastStageFilename;
		String sortedFile = lastStageFilename+"sorted";
		String cmd = "sort "+ lastStageFilename+" > "+sortedFile;
        Process p = Runtime.getRuntime().exec(new String[]{"bash","-c",cmd});
		int ret_code = -1;
		try {
			ret_code = p.waitFor();
        } catch (Exception e) {
			e.printStackTrace();
		}
		reader = new BufferedReader(new FileReader(sortedFile));
        this.converter = new StringFeedToBitsetFeedConverter(dimMappingFile,dimValMappingFile,true);
	}

	private String makeKey(String [] temp) {
		String key = "";
		for(int i=0;i<stageMinus2;i++){
				key +=  temp[i] + " " ;		
		}

		return key;
	}
	
	public String generatePartitionFile() throws IOException{
		
		
		String[] temp;
		if(line==null)
			return null;
		if(line.equals("")){
			
			if((line=reader.readLine())==null)
				return null;
			else{
				temp = line.split(Util.separatorBetweenFields);
				key = makeKey(temp);
				prevKey = key;
			}
		}
		
		String partitionFilename = "partitionFile.tmp"; // use same file
		
		BufferedWriter partitionFile = new BufferedWriter(new FileWriter(partitionFilename));
	
		noLines = 0;
		while(key.equals(prevKey)){
			noLines++;
            String bitsetLine = converter.StringRowToBitsetRow(line);
            partitionFile.write(bitsetLine);
			partitionFile.flush();
			line = reader.readLine();
			if(line==null){
				break;
			}	
			temp = line.split(Util.separatorBetweenFields);
			key = makeKey(temp);
		}
		partitionFile.close();
		prevKey = key;
		this.partitionCount = this.partitionCount + 1;
		return partitionFilename;
	}
	
	public static void main(String args[]) throws IOException{
		int stage = 5;
		String filename = "/Users/nitinkau/debug_tp2014/tp2014/src/4_stage_string_output.tmp";
		// input should be  _stage_string_output.tmp
		// insert this code in feader
		// convert the output(partition file) to bitset format in the while loop itself
		// call candidate generate
        Partition p = null;
        try {
            p = new Partition(filename,stage, "/Users/nitinkau/debug_tp2014/tp2014/src/dim_mapping.tmp","/Users/nitinkau/debug_tp2014/tp2014/src/dim_val_mapping.tmp");
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        String partfile = p.generatePartitionFile();
		int count= 0;
		while(partfile!=null){
			 count++;
			 partfile = p.generatePartitionFile();
			 if(p.noLines <= 1)
				 continue;
		}
	}
}
