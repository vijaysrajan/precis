package com.precis.aprioriOnAggregates.singleMachine;

import java.io.*;
import java.util.BitSet;

public class StringFeedToBitsetFeedConverter implements FeederToMapperIface{
    BufferedWriter writer;
    DimLookupDict dimLookupDict;
    boolean doesContainMetric;

    public StringFeedToBitsetFeedConverter(String dimMappingFile, String dimValMappingFile, boolean doesContainMetric) throws Exception {
        this.dimLookupDict = DimLookupDict.generateMapFromMappingFiles(dimMappingFile,dimValMappingFile);
        this.doesContainMetric = doesContainMetric;
    }

    public String StringRowToBitsetRow(String line) {
        String[] cols = line.split(String.valueOf(Util.separatorBetweenFields));
        BitSet bitSet = new BitSet(dimLookupDict.getDimBitsetLength()+dimLookupDict.getValBitsetLength());
        int outboundIndex = cols.length;
        if(this.doesContainMetric) {
            outboundIndex = outboundIndex-1;
        }
        for (int i=0; i<outboundIndex; i++) {
            String dim = cols[i].split(String.valueOf(Util.separatorBetweenDimAndVal))[0];
            String val = cols[i].split(String.valueOf(Util.separatorBetweenDimAndVal))[1];
            bitSet.set(dimLookupDict.getBitIndexForDim(dim));
            bitSet.set(dimLookupDict.getDimBitsetLength()+dimLookupDict.getBitIndexForDimValue(dim,val));
        }

        String writeLine = bitSet.toString();
        if (this.doesContainMetric) {
            writeLine += String.valueOf(Util.separatorBetweenFields);
            writeLine += cols[cols.length-1];
        }
        writeLine += "\n";
        return writeLine;
    }

    public StringFeedToBitsetFeedConverter(String bitsetFeedFilename, String dimMappingFile, String dimValMappingFile, boolean doesContainMetric) throws Exception {
        File writeFile = new File(bitsetFeedFilename);
        if (!writeFile.exists()) {
            writeFile.createNewFile();
        }
        this.writer = new BufferedWriter(new FileWriter(writeFile));
        this.dimLookupDict = DimLookupDict.generateMapFromMappingFiles(dimMappingFile,dimValMappingFile);
        this.doesContainMetric = doesContainMetric;
    }

    public void evaluate(String line) {
        String[] cols = line.split(String.valueOf(Util.separatorBetweenFields));
        BitSet bitSet = new BitSet(dimLookupDict.getDimBitsetLength()+dimLookupDict.getValBitsetLength());
        int outboundIndex = cols.length;
        if(this.doesContainMetric) {
            outboundIndex = outboundIndex-1;
        }
        for (int i=0; i<outboundIndex; i++) {
            String dim = cols[i].split(String.valueOf(Util.separatorBetweenDimAndVal))[0];
            String val = cols[i].split(String.valueOf(Util.separatorBetweenDimAndVal))[1];
            bitSet.set(dimLookupDict.getBitIndexForDim(dim));
            bitSet.set(dimLookupDict.getDimBitsetLength()+dimLookupDict.getBitIndexForDimValue(dim,val));
        }

        String writeLine = bitSet.toString();
        //Use StringBuilder
        if (this.doesContainMetric) {
            writeLine += String.valueOf(Util.separatorBetweenFields);
            writeLine += cols[cols.length-1];
        }
        writeLine += "\n";
        try {
            writer.write(writeLine);
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

    public void done() {
        try {
            this.writer.flush();
            this.writer.close();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public static void main(String[] args) {
        try {
            StringFeedToBitsetFeedConverter converter = new StringFeedToBitsetFeedConverter("/Users/nitinkau/tp2014/src/bitsetRedOp1.tmp","/Users/nitinkau/tp2014/src/dim_mapping.tmp","/Users/nitinkau/tp2014/src/dim_val_mapping.tmp",true);

            BufferedReader reader = new BufferedReader(new FileReader("/Users/nitinkau/tp2014/src/RedOp1.tmp"));
            String line = reader.readLine();
            while (line!=null) {
                converter.evaluate(line);
                line = reader.readLine();
            }
            converter.done();
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

}
