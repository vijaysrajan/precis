package com.precis.aprioriOnAggregates.singleMachine;

import java.io.*;
import java.util.BitSet;
import java.util.List;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.math.BigDecimal;
import java.text.DecimalFormat;



import static com.precis.aprioriOnAggregates.singleMachine.Util.redOp_Stage1;
import static com.precis.aprioriOnAggregates.singleMachine.Util.separatorBetweenFields;

public class CandidateFilter implements FeederToMapperIface{
    private DimLookupDict dimLookupDict;
    private BufferedWriter writer;
    private Integer stage;
    private Double threshold;
    public Integer recordsWritten;
    String basicFeedFilename;
    static List<String> theWholebasicFeedFileInList = null;
    static String [] baseDataInMemory = null;
    private BitsetFeedToStringFeedConverter converter;
    DecimalFormat df = null;

    private static boolean IN_MEM = true;

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

    public CandidateFilter(Integer stage, String dimMappingFilename, String dimValMappingFilename, String basicFeedFilename, String stageOutputFilename,Double threshold) throws Exception {
        this.stage = stage;
        this.threshold = threshold;
        this.dimLookupDict = DimLookupDict.generateMapFromMappingFiles(dimMappingFilename,dimValMappingFilename);
        this.basicFeedFilename = basicFeedFilename;
        this.recordsWritten = 0;
        File writeFile = new File(stageOutputFilename);
        if (!writeFile.exists()) {
            writeFile.createNewFile();
        }
        this.writer = new BufferedWriter(new FileWriter(writeFile));
        converter = new BitsetFeedToStringFeedConverter(dimMappingFilename,dimValMappingFilename,true);
        df = new DecimalFormat("#");
        df.setMaximumFractionDigits(1);
        if (IN_MEM == true) { 
            if (theWholebasicFeedFileInList == null) {
	          theWholebasicFeedFileInList = Files.readAllLines(Paths.get(basicFeedFilename), Charset.defaultCharset());
                  baseDataInMemory = new String[theWholebasicFeedFileInList.size()];
                  baseDataInMemory = theWholebasicFeedFileInList.toArray(baseDataInMemory);
	    }
        } 
    }

    int failedEvaluations = 0;
    int successfulEvaluations = 0;
    //CHANGE IN MEM VS BufferedReader
    private void evaluateInMem(String line) {
        BitSet candidate = this.bitSetStringToBitset(line,this.dimLookupDict.getDimBitsetLength()+this.dimLookupDict.getValBitsetLength());
        ComparableBitSet comparableBitSet = new ComparableBitSet(candidate);
        Double metric = new Double(0.0);
        try {
               String tline = null;
               //Iterator<String> itrStr = theWholebasicFeedFileInList.iterator();
               //while (itrStr.hasNext()){
                       //tline = itrStr.next();
               for (int i = 0; i < baseDataInMemory.length; i++) {
                       tline = baseDataInMemory[i]; 
                       String[] cols = tline.split(separatorBetweenFields);
                       BitSet bitSet = this.bitSetStringToBitset(cols[0], this.dimLookupDict.getDimBitsetLength() + this.dimLookupDict.getValBitsetLength());
                       bitSet.and(candidate);
                       ComparableBitSet comparableBitSet1 = new ComparableBitSet(bitSet);
                       if (comparableBitSet1.compareTo(comparableBitSet)==0) {
                             metric = metric + Double.parseDouble(cols[1]);
                             if (metric >= this.threshold) {
                                   break;
                             }
                       }
               }
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        if (metric>=threshold) {
            try {
                this.recordsWritten = this.recordsWritten + 1;
                writer.write(this.converter.convertBitsetRowToStringRow(line+separatorBetweenFields+String.valueOf(df.format(metric))));
                writer.flush();
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            successfulEvaluations++;
        } else  {
            failedEvaluations++; 
        }
    }


    public void evaluateFileIO(String line) {
        BitSet candidate = this.bitSetStringToBitset(line,this.dimLookupDict.getDimBitsetLength()+this.dimLookupDict.getValBitsetLength());
        ComparableBitSet comparableBitSet = new ComparableBitSet(candidate);
        Double metric = new Double(0.0);
        try {
              BufferedReader reader = new BufferedReader(new FileReader(this.basicFeedFilename));
              String tline = reader.readLine();
              while (tline!=null) {
                String[] cols = tline.split(separatorBetweenFields);
                BitSet bitSet = this.bitSetStringToBitset(cols[0], this.dimLookupDict.getDimBitsetLength() + this.dimLookupDict.getValBitsetLength());
                bitSet.and(candidate);
                ComparableBitSet comparableBitSet1 = new ComparableBitSet(bitSet);
                if (comparableBitSet1.compareTo(comparableBitSet)==0) {
                    metric = metric + Double.parseDouble(cols[1]);
                    if (metric >= this.threshold) {
                         break;
                    }
                }
                tline = reader.readLine();
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        if (metric>=threshold) {
            try {
                this.recordsWritten = this.recordsWritten + 1;
                //writer.write(stage+separatorBetweenFields+this.converter.convertBitsetRowToStringRow(line+separatorBetweenFields+String.valueOf(df.format(metric))));
                writer.write(this.converter.convertBitsetRowToStringRow(line+separatorBetweenFields+String.valueOf(df.format(metric))));
                writer.flush();
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    }



    @Override
    public void evaluate(String line) {
          if (IN_MEM == true) {
               evaluateInMem(line);
          } else {
               evaluateFileIO(line);
          } 
    }

    @Override
    public void done() {
        System.out.println("failed evaluation = " + failedEvaluations + ";      successfulEvaluations = " + successfulEvaluations);
        try {
            this.writer.flush();
            this.writer.close();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public static void main(String[] args) {
        try {
            CandidateFilter filter = new CandidateFilter(2,"dim_mapping.tmp","dim_val_mapping.tmp","baseFeedOutput","secondStageOutput.tmp",5.0);
            BufferedReader reader = new BufferedReader(new FileReader("secondStageCandidate.tmp"));
            String line = reader.readLine();
            while (line!=null) {
                filter.evaluate(line);
                line = reader.readLine();
            }
            filter.done();
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
