package com.precis.aprioriOnAggregates.singleMachine;

import java.io.*;
import java.util.BitSet;
import java.util.Map;

public class BitsetFeedToStringFeedConverter implements FeederToMapperIface{
    BufferedWriter writer;
    DimLookupDict dimLookupDict;
    boolean doesContainMetric;

    public BitsetFeedToStringFeedConverter(String dimMappingFile, String dimValMappingFile,boolean doesContainMetric) throws Exception {
        this.doesContainMetric = doesContainMetric;
        this.dimLookupDict = DimLookupDict.generateMapFromMappingFiles(dimMappingFile,dimValMappingFile);
    }

    public String convertBitsetRowToStringRow(String line) {
        String writeLine = "";
        String bitsetString = line.split(String.valueOf(Util.separatorBetweenFields))[0];
        BitSet bitSet = bitSetStringToBitset(bitsetString,dimLookupDict.getDimBitsetLength()+dimLookupDict.getValBitsetLength());
        Integer setBitIndex = bitSet.nextSetBit(dimLookupDict.getDimBitsetLength());
        while (setBitIndex!=-1) {
            Map<String,String > tmap = dimLookupDict.getDimValueForBitINdex(setBitIndex-dimLookupDict.getDimBitsetLength());
            for (Map.Entry<String,String> entry : tmap.entrySet()) {
                writeLine += (entry.getKey()+String.valueOf(Util.separatorBetweenDimAndVal)+entry.getValue())+Util.separatorBetweenFields;
            }
            setBitIndex = bitSet.nextSetBit(setBitIndex+1);
            if (setBitIndex!=-1) {
                Integer tsetBitIndex = bitSet.nextSetBit(setBitIndex+1);
                if (tsetBitIndex!=-1) {
                    //writeLine += Util.separatorBetweenFields;
                }
            }
        }
        writeLine = writeLine.substring(0,writeLine.length()-1);
        if(this.doesContainMetric) {
            writeLine += Util.separatorBetweenFields;
            writeLine += line.split(String.valueOf(Util.separatorBetweenFields))[1];
        }
        writeLine += "\n";
        return writeLine;
    }

    public BitsetFeedToStringFeedConverter(String stringFeedFilename, String dimMappingFile, String dimValMappingFile, boolean doesContainMetric) throws Exception {
        File writeFile = new File(stringFeedFilename);
        if (!writeFile.exists()) {
            writeFile.createNewFile();
        }
        this.writer = new BufferedWriter(new FileWriter(writeFile));
        this.dimLookupDict = DimLookupDict.generateMapFromMappingFiles(dimMappingFile,dimValMappingFile);
        this.doesContainMetric = doesContainMetric;
    }
    private BitSet bitSetStringToBitset(String bitsetString, int size) {
        BitSet res = new BitSet(size);
        bitsetString = bitsetString.substring(1,bitsetString.length()-1);
        String[] nums = bitsetString.split(",");
        for (int i=0;i<nums.length;i++) {
            String num = nums[i];
            num = num.replaceAll(" ","");
            res.set(Integer.parseInt(num));
        }
        return res;
    }

    public void evaluate(String line) {
        String writeLine = "";
        String bitsetString = line.split(String.valueOf(Util.separatorBetweenFields))[0];
        BitSet bitSet = bitSetStringToBitset(bitsetString,dimLookupDict.getDimBitsetLength()+dimLookupDict.getValBitsetLength());
        Integer setBitIndex = bitSet.nextSetBit(dimLookupDict.getDimBitsetLength());
        while (setBitIndex!=-1) {
            Map<String,String > tmap = dimLookupDict.getDimValueForBitINdex(setBitIndex-dimLookupDict.getDimBitsetLength());
            for (Map.Entry<String,String> entry : tmap.entrySet()) {
                writeLine += (entry.getKey()+String.valueOf(Util.separatorBetweenDimAndVal)+entry.getValue())+Util.separatorBetweenFields;
            }
            setBitIndex = bitSet.nextSetBit(setBitIndex+1);
            if (setBitIndex!=-1) {
                Integer tsetBitIndex = bitSet.nextSetBit(setBitIndex+1);
                if (tsetBitIndex!=-1) {
                    //writeLine += Util.separatorBetweenFields;
                }
            }
        }
        writeLine = writeLine.substring(0,writeLine.length()-1);
        if(this.doesContainMetric) {
            writeLine += Util.separatorBetweenFields;
            writeLine += line.split(String.valueOf(Util.separatorBetweenFields))[1];
        }
        writeLine += "\n";
        try {
            this.writer.write(writeLine);
            this.writer.flush();
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

    public void convertFeed(String readfilename) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(readfilename));
            String line = reader.readLine();
            while (line!=null) {
                this.evaluate(line);
                line = reader.readLine();
            }
            this.done();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
