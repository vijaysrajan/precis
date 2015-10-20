package com.precis.aprioriOnAggregates.singleMachine;

import java.io.*;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

public class DataGeneratorMapReduce implements FeederToMapperIface {

    private DimLookupDict dimLookupDict;
    private BufferedReader reader;
    private BufferedWriter writer;
    private Set<ComparableBitSet> set;
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

    public DataGeneratorMapReduce(String feedFilename, String outputFeedFilename, String dimMapping, String dimValMapping) throws Exception {
        this.reader = new BufferedReader(new FileReader(feedFilename));
        this.writer = new BufferedWriter(new FileWriter(outputFeedFilename));
        this.dimLookupDict = DimLookupDict.generateMapFromMappingFiles(dimMapping,dimValMapping);
        this.set = new HashSet<ComparableBitSet>();
    }

    @Override
    public void evaluate(String line) {
        try {
            BitSet bitSet = bitSetStringToBitset(line.split(Util.separatorBetweenFields)[0],(dimLookupDict.getDimBitsetLength()+dimLookupDict.getValBitsetLength()));
            BitSet dimBitset = bitSet.get(0,dimLookupDict.getDimBitsetLength());
            BitSet valBitset = bitSet.get(dimLookupDict.getDimBitsetLength(),(dimLookupDict.getDimBitsetLength()+dimLookupDict.getValBitsetLength()));
            String tline = this.reader.readLine();
            while (tline!=null) {
                BitSet tbitset = bitSetStringToBitset(tline.split(Util.separatorBetweenFields)[0],(dimLookupDict.getDimBitsetLength()+dimLookupDict.getValBitsetLength()));
                BitSet tdimBitset = tbitset.get(0,dimLookupDict.getDimBitsetLength());
                tdimBitset.xor(dimBitset);
                BitSet tvalBitset = tbitset.get(dimLookupDict.getDimBitsetLength(),(dimLookupDict.getDimBitsetLength()+dimLookupDict.getValBitsetLength()));
                tvalBitset.xor(valBitset);
                if (tdimBitset.cardinality()==2 && tvalBitset.cardinality()==2) {
                    tbitset.or(bitSet);
                    set.add(new ComparableBitSet(tbitset));
                }
                tline = this.reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Override
    public void done() {
        try {
            this.reader.close();
            System.out.println(this.set);
            for (ComparableBitSet comparableBitSet : this.set) {
                this.writer.write(comparableBitSet.bitSet.toString()+"\n");
                this.writer.flush();
            }
            this.writer.close();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public static void main(String[] args) {
        try {
            DataGeneratorMapReduce generatorMapReduce = new DataGeneratorMapReduce("bitsetRedOp1.tmp","secondStageCandidate.tmp","dim_mapping.tmp","dim_val_mapping.tmp");
            BufferedReader reader = new BufferedReader(new FileReader("bitsetRedOp1.tmp"));
            String line = reader.readLine();
            while (line!=null) {
                generatorMapReduce.evaluate(line);
                line = reader.readLine();
            }
            generatorMapReduce.done();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
