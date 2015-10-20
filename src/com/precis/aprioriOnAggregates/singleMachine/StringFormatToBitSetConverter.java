package com.precis.aprioriOnAggregates.singleMachine;

import java.io.*;
import java.util.BitSet;

/**
 * Created with IntelliJ IDEA.
 * User: nitinkau
 * Date: 6/23/14
 * Time: 6:26 PM
 * To change this template use File | Settings | File Templates.
 */
public class StringFormatToBitSetConverter {
    private DimLookupDict dimLookupDict;
    private String inputFilename;
    private String outputfilename;

    public StringFormatToBitSetConverter(DimLookupDict dimLookupDict, String inputFilename, String outputfilename) {
        this.dimLookupDict = dimLookupDict;
        this.inputFilename = inputFilename;
        this.outputfilename = outputfilename;
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
            String[] cols = line.split(String.valueOf(Util.separatorBetweenFields));
            BitSet bitSet = new BitSet(dimLookupDict.getDimBitsetLength()+dimLookupDict.getValBitsetLength());
            int outboundIndex = cols.length;
            if(doesHaveMetric) {
                outboundIndex = outboundIndex-1;
            }
            for (int i=0; i<outboundIndex; i++) {
                String dim = cols[i].split(String.valueOf(Util.separatorBetweenDimAndVal))[0];
                String val = cols[i].split(String.valueOf(Util.separatorBetweenDimAndVal))[1];
                bitSet.set(dimLookupDict.getBitIndexForDim(dim));
                bitSet.set(dimLookupDict.getDimBitsetLength()+dimLookupDict.getBitIndexForDimValue(dim,val));
            }

            String writeLine = bitSet.toString();
            if (doesHaveMetric) {
                writeLine += String.valueOf(Util.separatorBetweenFields);
                writeLine += cols[cols.length-1];
            }
            writeLine += "\n";
            writer.write(writeLine);
            writer.flush();
            line = reader.readLine();
        }
        writer.close();
    }

    public static void main(String[] args) {
        try {
            DimLookupDict.setInputFile("/Users/nitinkau/tp2014/src/RedOp");
            DimLookupDict.setDimMappingFile("/Users/nitinkau/tp2014/src/dimMapping");
            DimLookupDict.setValMappingFile("/Users/nitinkau/tp2014/src/valMapping");
            DimLookupDict dimLookupDict = DimLookupDict.getInstanceForInputfile();
            StringFormatToBitSetConverter converter = new StringFormatToBitSetConverter(dimLookupDict,"/Users/nitinkau/tp2014/src/RedOp","/Users/nitinkau/tp2014/src/bitsetOutput1");
            converter.convert(true);
        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

}
