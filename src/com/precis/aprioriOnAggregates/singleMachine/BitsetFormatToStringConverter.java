package com.precis.aprioriOnAggregates.singleMachine;

import java.io.*;
import java.util.BitSet;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: nitinkau
 * Date: 6/23/14
 * Time: 6:58 PM
 * To change this template use File | Settings | File Templates.
 */
public class BitsetFormatToStringConverter {
    private DimLookupDict dimLookupDict;
    private String inputFilename;
    private String outputfilename;

    public BitsetFormatToStringConverter(DimLookupDict dimLookupDict, String inputFilename, String outputfilename) {
        this.dimLookupDict = dimLookupDict;
        this.inputFilename = inputFilename;
        this.outputfilename = outputfilename;
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

    public void convert(boolean doesHaveMetric) throws IOException {
        File writeFile = new File(this.outputfilename);
        if (!writeFile.exists()) {
            writeFile.createNewFile();
        }
        BufferedReader reader = new BufferedReader(new FileReader(this.inputFilename));
        BufferedWriter writer = new BufferedWriter(new FileWriter(writeFile));
        String line = reader.readLine();
        while (line!=null) {
            String writeLine = "";
            String bitsetString = line.split(String.valueOf(Util.separatorBetweenFields))[0];
            BitSet bitSet = bitSetStringToBitset(bitsetString,dimLookupDict.getDimBitsetLength()+dimLookupDict.getValBitsetLength());
            Integer setBitIndex = bitSet.nextSetBit(dimLookupDict.getDimBitsetLength());
            while (setBitIndex!=-1) {
                Map<String,String > tmap = dimLookupDict.getDimValueForBitINdex(setBitIndex-dimLookupDict.getDimBitsetLength());
                for (Map.Entry<String,String> entry : tmap.entrySet()) {
                    writeLine += (entry.getKey()+String.valueOf(Util.separatorBetweenDimAndVal)+entry.getValue());
                }
                writeLine += String.valueOf(Util.separatorBetweenFields);
            }
        }
    }

    public static void main(String[] args) {
        try {
            DimLookupDict.setInputFile("RedOp");
            DimLookupDict.setDimMappingFile("dimMapping");
            DimLookupDict.setValMappingFile("valMapping");
            DimLookupDict dimLookupDict = DimLookupDict.getInstanceForInputfile();
            StringFormatToBitSetConverter converter = new StringFormatToBitSetConverter(dimLookupDict,"RedOp","bitsetOutput1");
            converter.convert(true);
        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

}
