package com.precis.aprioriOnAggregates.singleMachine;


import java.io.*;
import java.util.BitSet;

/**
 * Created with IntelliJ IDEA.
 * User: nitinkau
 * Date: 6/23/14
 * Time: 5:34 PM
 * To change this template use File | Settings | File Templates.
 */
public class CandidateGenerator {
    private DimLookupDict dimLookupDict;
    private Integer stageNUmber;
    private String lastStageFile;
    private String currentStageCandidateFile;
    private BitSet _res =  null;
    private static BitSet _all0s = null;

    public CandidateGenerator(DimLookupDict dimLookupDict, Integer stageNUmber, String lastStageFile, String currentStageCandidateFile) {
        this.dimLookupDict = dimLookupDict;
        this.stageNUmber = stageNUmber;
        this.lastStageFile = lastStageFile;
        this.currentStageCandidateFile = currentStageCandidateFile;
        this._res   = new BitSet(dimLookupDict.getDimBitsetLength()+dimLookupDict.getValBitsetLength());
        _all0s = new BitSet(dimLookupDict.getDimBitsetLength()+dimLookupDict.getValBitsetLength());
    }

    private BitSet bitSetStringToBitset(String bitsetString, int size) {
        //BitSet res = new BitSet(size);
        _res.and(_all0s);
        bitsetString = bitsetString.substring(1,bitsetString.length()-1);
        String[] nums = bitsetString.split(",");
        for (int i=0;i<nums.length;i++) {
            String num = nums[i];
            num = num.replaceAll(" ","");
            _res.set(Integer.parseInt(num));
        }
        return _res;
    }

    public void generateCandidates() throws IOException {
        File bitsetOutputFile = new File(this.currentStageCandidateFile);
        if (!bitsetOutputFile.exists()) {
            bitsetOutputFile.createNewFile();
        }
        BufferedReader reader = new BufferedReader(new FileReader(this.lastStageFile));
        BufferedWriter bitsetOutputWriter = new BufferedWriter(new FileWriter(bitsetOutputFile));
        String line = reader.readLine();
        int lineIndex=0;
        while (line!=null) {
            BitSet combinedBitset = this.bitSetStringToBitset(line.split(String.valueOf(Util.separatorBetweenFields))[0],dimLookupDict.getDimBitsetLength()+dimLookupDict.getValBitsetLength());
            BitSet dimBitset = combinedBitset.get(0,dimLookupDict.getDimBitsetLength());
            BitSet valBitset = combinedBitset.get(dimLookupDict.getDimBitsetLength(),dimLookupDict.getDimBitsetLength()+dimLookupDict.getValBitsetLength());
            BufferedReader treader = new BufferedReader(new FileReader(currentStageCandidateFile));
            String tline = null;
            for (int i=0; i<=lineIndex; i++) {
                tline = treader.readLine();
            }
            tline = treader.readLine();
            while (tline!=null) {
                BitSet tcombinedBitset = this.bitSetStringToBitset(tline.split(String.valueOf(Util.separatorBetweenFields))[0],dimLookupDict.getDimBitsetLength()+dimLookupDict.getValBitsetLength());
                BitSet tdimBitset = tcombinedBitset.get(0,dimLookupDict.getDimBitsetLength());
                BitSet tvalBitset = tcombinedBitset.get(dimLookupDict.getDimBitsetLength(),dimLookupDict.getDimBitsetLength()+dimLookupDict.getValBitsetLength());
                tdimBitset.xor(dimBitset);
                tvalBitset.xor(valBitset);
                if (tdimBitset.cardinality()==2 && tvalBitset.cardinality()==2) {
                    tcombinedBitset.or(combinedBitset);
                    bitsetOutputWriter.write(tcombinedBitset.toString()+"\n");
                    bitsetOutputWriter.flush();
                }
                tline = treader.readLine();
            }
            line = reader.readLine();
            lineIndex++;
        }
        bitsetOutputWriter.close();

    }

    public static void main(String args[]) {

    }

}
