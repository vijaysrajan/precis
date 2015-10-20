package com.precis.aprioriOnAggregates.singleMachine.bitsetEvaluation;

import javafx.util.Pair;

import java.io.*;
import java.util.BitSet;

/**
 * Created with IntelliJ IDEA.
 * User: nitinkau
 * Date: 6/20/14
 * Time: 9:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class CandidateGenerator {
    private DimMapper dimMapper;
    private String inputFilename;
    private String bitsetOutputFilename;
    private String stringOutputFilename;

    public CandidateGenerator(DimMapper dimMapper, String inputFilename, String bitsetOutputFilename, String stringOutputFilename) {
        this.dimMapper = dimMapper;
        this.inputFilename = inputFilename;
        this.bitsetOutputFilename = bitsetOutputFilename;
        this.stringOutputFilename = stringOutputFilename;
    }

    public void generateCandidate() throws IOException {
        File bitsetOutputFile = new File(this.bitsetOutputFilename);
        if (!bitsetOutputFile.exists()) {
            bitsetOutputFile.createNewFile();
        }
        File stringOutputFile = new File(this.stringOutputFilename);
        if (!stringOutputFile.exists()) {
            stringOutputFile.createNewFile();
        }
        BufferedReader reader = new BufferedReader(new FileReader(inputFilename));
        BufferedWriter bitsetOutputWriter = new BufferedWriter(new FileWriter(bitsetOutputFile));
        BufferedWriter stringOutputWriter = new BufferedWriter(new FileWriter(stringOutputFile));
        String line = reader.readLine();
        int lineIndex=0;
        while (line!=null) {
            Pair<BitSet,BitSet> pair = dimMapper.bitToDimVal(line);
            BitSet dimBitset = pair.getKey();
            BitSet bitSet1 = dimMapper.bitToVal(line).getKey();
            BitSet valBitset = pair.getValue();
            BufferedReader treader = new BufferedReader(new FileReader(inputFilename));
            String tline = null;
            for (int i=0; i<=lineIndex; i++) {
                tline = treader.readLine();
            }
            tline = treader.readLine();
            while (tline!=null) {
                Pair<BitSet,BitSet> bitSetPair = dimMapper.bitToDimVal(tline);
                BitSet tdimBitset = bitSetPair.getKey();
                BitSet tvalBitset = bitSetPair.getValue();
                tdimBitset.xor(dimBitset);
                tvalBitset.xor(valBitset);
                if (tdimBitset.cardinality()==2 && tvalBitset.cardinality()==2) {
                    BitSet bitSet2 = dimMapper.bitToVal(tline).getKey();
                    bitSet2.or(bitSet1);
                    System.out.println(bitSet2.toString());
                    bitsetOutputWriter.write(bitSet2.toString() + "\n");
                    bitsetOutputWriter.flush();
                    Pair<String,String>[] pairs = dimMapper.bitsetToDimVal(bitSet2);
                    String outputString = "";
                    for (int i=0; i<pairs.length; i++) {
                        outputString += (pairs[i].getKey()+":"+pairs[i].getValue()+"\u0001");
                    }
                    stringOutputWriter.write(outputString+"\n");
                    stringOutputWriter.flush();
                }
                tline = treader.readLine();
            }
            line = reader.readLine();
            lineIndex++;
        }
        bitsetOutputWriter.close();
        stringOutputWriter.close();
    }

    public static void main(String[] args) {
        DimMapper dimMapper = new DimMapper("test.txt","first_round.txt","bitTodim.txt","bitToval.txt");
        try {
            dimMapper.doMapping();
            dimMapper.putToBit();
            CandidateGenerator candidateGenerator = new CandidateGenerator(dimMapper,"first_round.txt","bitsetOutput.txt","gstringOutput.txt");
            candidateGenerator.generateCandidate();
            CandidateGenerator candidateGenerator1 = new CandidateGenerator(dimMapper,"bitsetOutput.txt","bitsetOutput_second_stage.txt","gstringOutputSecondStage.txt");
            candidateGenerator1.generateCandidate();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

}
