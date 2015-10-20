package com.precis.aprioriOnAggregates.singleMachine.bitsetEvaluation;

import javafx.util.Pair;

import java.io.*;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

public class DimMapper {
    private Map<Integer,String> bitToDim;
    private Map<Integer,Pair<String,String>> bitToVal;
    private Map<String,Integer> dimToBit;
    private Map<String,Map<String,Integer>> valToBit;
    private String inputFile;
    private String outputFile;
    private String dimMappingFile;
    private String valMappingFile;
    private Integer byteArrayLength;

    public DimMapper(String inputFileName, String outputFileName, String valMappingFileName, String dimMappingFileName) {
        this.bitToDim = new HashMap<Integer, String>();
        this.bitToVal = new HashMap<Integer, Pair<String, String>>();
        this.dimToBit = new HashMap<String, Integer>();
        this.valToBit = new HashMap<String, Map<String, Integer>>();
        this.inputFile = inputFileName;
        this.outputFile = outputFileName;
        this.dimMappingFile = dimMappingFileName;
        this.valMappingFile = valMappingFileName;
    }

    /**
     * Create the four hashes
     * Store the mappings into respective files
     * @throws IOException
     */
    public void doMapping() throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        String line = reader.readLine();
        Integer dimIndex=0;
        Integer valIndex=0;
        while (line!=null) {
            String[] cols = line.split("\u0001");
            String dim = cols[0];
            String val = cols[1];
            if (dimToBit.get(dim)==null) {
                dimToBit.put(dim,dimIndex);
                bitToDim.put(dimIndex,dim);
                valToBit.put(dim,new HashMap<String, Integer>());
                dimIndex++;
            }
            if (valToBit.get(dim).get(val)==null) {
                valToBit.get(dim).put(val,valIndex);
                Pair<String,String> pair = new Pair<String, String>(dim,val);
                bitToVal.put(valIndex,pair);
                valIndex++;
            }
            line=reader.readLine();
        }
        File dimMappingFile = new File(this.dimMappingFile);
        if (!dimMappingFile.exists()) {
            dimMappingFile.createNewFile();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(dimMappingFile));
        for (Map.Entry<Integer,String> entry : this.bitToDim.entrySet()) {
            writer.write(entry.getKey()+"\u0001"+entry.getValue()+"\n");
        }
        writer.close();

        File valMappingFile = new File(this.valMappingFile);
        if (!valMappingFile.exists()) {
            valMappingFile.createNewFile();
        }
        writer = new BufferedWriter(new FileWriter(valMappingFile));
        for (Map.Entry<Integer,Pair<String,String>> entry : this.bitToVal.entrySet()) {
            writer.write(entry.getKey()+"\u0001"+entry.getValue().getKey()+"\u0001"+entry.getValue().getValue()+"\n");
        }
        writer.close();
    }

    /**
     * Will Store the bitset as toString() to the output file
     * @throws IOException
     */
    public void putToBit() throws IOException {
        File writeFile = new File(this.outputFile);
        if (!writeFile.exists()) {
            writeFile.createNewFile();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(writeFile));
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        String line = reader.readLine();
        while (line!=null) {
            String cols[] = line.split("\u0001");
            String dim = cols[0];
            String val = cols[1];
            double metric = Double.parseDouble(cols[2]);
            BitSet bitSet = new BitSet(bitToDim.size()+bitToVal.size());
            bitSet.set(dimToBit.get(dim));
            bitSet.set(dimToBit.size()+valToBit.get(dim).get(val));
            line = reader.readLine();
            writer.write(bitSet.toString()+"\u0001"+metric+"\n");
        }
        reader.close();
        writer.close();
    }

    public Pair<BitSet,Double> bitToVal(String line) throws IOException {
        String bitsetString = line.split("\u0001")[0];
        BitSet res = this.bitSetStringToBitset(bitsetString,bitToDim.size()+bitToVal.size());
        return new Pair<BitSet, Double>(res,Double.parseDouble(line.split("\u0001")[1]));
    }

    public Pair<BitSet,BitSet> bitToDimVal(String line) {
        String bitsetString = line.split("\u0001")[0];
        BitSet res = this.bitSetStringToBitset(bitsetString,bitToDim.size()+bitToVal.size());
        BitSet dimBitSet = res.get(0,bitToDim.size());
        BitSet valBitSet = res.get(bitToDim.size(),res.size());
        return new Pair<BitSet, BitSet>(dimBitSet,valBitSet);
    }

    public Pair<String,String>[] bitsetToDimVal(BitSet bitSet) {
        int totalDim = bitSet.cardinality();
        totalDim = totalDim/2;
        Pair<String,String>[] pairs = new Pair[totalDim];
        int index = bitSet.nextSetBit(bitToDim.size());
        int i=0;
        while (index!=-1) {
            pairs[i] = bitToVal.get(index-bitToDim.size());
            index = bitSet.nextSetBit(index+1);
            i++;
        }
        return pairs;
    }

    public BitSet bitSetStringToBitset(String bitsetString, int size) {
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

    public static void main(String args[]) {
        DimMapper dimMapper = new DimMapper("test.txt","outputTest.txt","valMapping.txt","dimMapping.txt");
        try {
            dimMapper.doMapping();
            dimMapper.putToBit();
            BufferedReader reader = new BufferedReader(new FileReader("outputTest.txt"));
            String line = reader.readLine();
            while (line!=null) {
                Pair<BitSet,Double> pair = null;
                pair = dimMapper.bitToVal(line);
                BitSet bitSet = pair.getKey();
                Pair<String,String>[] pairs = dimMapper.bitsetToDimVal(bitSet);
                for (int i=0; i<pairs.length; i++) {
                    System.out.println(pairs[i].getKey()+" "+pairs[i].getValue()+" "+pair.getValue());
                }
                line = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
