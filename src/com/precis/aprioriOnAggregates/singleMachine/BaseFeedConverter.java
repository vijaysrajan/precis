package com.precis.aprioriOnAggregates.singleMachine;

import java.io.*;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

public class BaseFeedConverter { 
    private String schemaFileName;
    private String basefeedFilename;
    private DimLookupDict dimLookupDict;
    private Map<Integer,String> dimToIndexMapping;
    private BufferedWriter bwOutputFeed;

    public BaseFeedConverter(String schemaFileName, String basefeedFilename, String outputfeedFilename, String dim_mapping, String dim_val_mapping) throws IOException {
        this.schemaFileName = schemaFileName;
        this.basefeedFilename = basefeedFilename;
        try {
            this.dimLookupDict = DimLookupDict.generateMapFromMappingFiles(dim_mapping,dim_val_mapping);
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        this.dimToIndexMapping = new HashMap<Integer, String>();
        this.mapDimToIndex();
        File writeFile = new File(outputfeedFilename);
        if (!writeFile.exists()) {
            writeFile.createNewFile();
        }
        this.bwOutputFeed = new BufferedWriter(new FileWriter(writeFile));
    }

    private void mapDimToIndex() throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(this.schemaFileName));
        String line = reader.readLine();
        Integer lineIndex=0;
        while (line!=null) {
            String[] cols = line.split(String.valueOf(Util.separatorBetweenFields));
            if (cols[1].equalsIgnoreCase("d")) {
                dimToIndexMapping.put(lineIndex,cols[0]);
            }
            lineIndex++;
            line = reader.readLine();
        }
    }

    public void convertFeed() throws IOException {
        BufferedWriter writer = this.bwOutputFeed;
        BufferedReader reader = new BufferedReader(new FileReader(this.basefeedFilename));
        String line = reader.readLine();
        while (line!=null) {
            String cols[] = line.split(String.valueOf(Util.separatorBetweenFields));
            BitSet bitSet = new BitSet(dimLookupDict.getDimBitsetLength()+dimLookupDict.getValBitsetLength());
            for (Map.Entry<Integer,String> entry:this.dimToIndexMapping.entrySet()) {
                String dim = entry.getValue();
                String val = cols[entry.getKey()];
                if (dimLookupDict.getBitIndexForDimValue(dim,val)!=null) {
                    Integer dimIndex = dimLookupDict.getBitIndexForDim(dim);
                    Integer valIndex = dimLookupDict.getBitIndexForDimValue(dim,val);
                    bitSet.set(dimIndex);
                    bitSet.set(dimLookupDict.getDimBitsetLength()+valIndex);
                }
            }
            if (bitSet.cardinality()>0) {
                String writeLine = bitSet.toString()+String.valueOf(Util.separatorBetweenFields)+cols[cols.length-1]+"\n";
                System.out.println(writeLine);
                writer.write(writeLine);
                writer.flush();
            }
            line = reader.readLine();
        }
    }

   public void evaluate (String line) throws IOException {
       String cols[] = line.split(String.valueOf(Util.separatorBetweenFields));
       BitSet bitSet = new BitSet(this.dimLookupDict.getDimBitsetLength()+this.dimLookupDict.getValBitsetLength());
       for (Map.Entry<Integer,String> entry:this.dimToIndexMapping.entrySet()) {
           String dim = entry.getValue();
           String val = cols[entry.getKey()];
           if (dimLookupDict.getBitIndexForDimValue(dim,val)!=null) {
               Integer dimIndex = dimLookupDict.getBitIndexForDim(dim);
               Integer valIndex = dimLookupDict.getBitIndexForDimValue(dim,val);
               bitSet.set(dimIndex);
               bitSet.set(dimLookupDict.getDimBitsetLength()+valIndex);
           }
       }
       if (bitSet.cardinality()>0) {
           String writeLine = bitSet.toString()+String.valueOf(Util.separatorBetweenFields)+cols[cols.length-1]+"\n";
           this.bwOutputFeed.write(writeLine);
           this.bwOutputFeed.flush();
       }
   }

   public void done() {

   }

}
