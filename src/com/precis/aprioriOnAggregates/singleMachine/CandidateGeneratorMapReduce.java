package com.precis.aprioriOnAggregates.singleMachine;

import java.io.*;
import java.util.*;

public class CandidateGeneratorMapReduce implements FeederToMapperIface {

    private DimLookupDict dimLookupDict;
    private String feedFilename;
    private BufferedWriter writer;
    private Set<ComparableBitSet> set;
    private BitSet _res1 =  null;
    private BitSet _res2 =  null;
    private static BitSet _all0s = null;

    private BitSet bitSetStringToBitset(String bitsetString, int size, BitSet _res) {
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

    public CandidateGeneratorMapReduce(String feedFilename, String outputFeedFilename, String dimMapping, String dimValMapping) throws Exception {
        this.feedFilename = feedFilename; 
        File file = new File(outputFeedFilename);
        if (!file.exists()) {
            file.createNewFile();
        }
        this.writer = new BufferedWriter(new FileWriter(file,true));

        //this.writer = new BufferedWriter(new FileWriter(outputFeedFilename));
        this.dimLookupDict = DimLookupDict.generateMapFromMappingFiles(dimMapping,dimValMapping);
        this._res1   = new BitSet(dimLookupDict.getDimBitsetLength()+dimLookupDict.getValBitsetLength());
        this._res2   = new BitSet(dimLookupDict.getDimBitsetLength()+dimLookupDict.getValBitsetLength());
        _all0s = new BitSet(dimLookupDict.getDimBitsetLength()+dimLookupDict.getValBitsetLength()); 
        this.set = new TreeSet<ComparableBitSet>(new Comparator<ComparableBitSet>() {
                       @Override
                       public int compare(ComparableBitSet o1, ComparableBitSet o2) {
                           return o1.compareTo(o2);
                       }
        });
    }

    @Override
    public void evaluate(String line) {
        try {
            BitSet bitSet = bitSetStringToBitset(line.split(Util.separatorBetweenFields)[0],(dimLookupDict.getDimBitsetLength()+dimLookupDict.getValBitsetLength()),_res1);
            BitSet dimBitset = bitSet.get(0,dimLookupDict.getDimBitsetLength());
            BitSet valBitset = bitSet.get(dimLookupDict.getDimBitsetLength(),(dimLookupDict.getDimBitsetLength()+dimLookupDict.getValBitsetLength()));
            BufferedReader reader = new BufferedReader(new FileReader(this.feedFilename));
            String tline = reader.readLine();
            while (tline!=null) {
                BitSet tbitset = bitSetStringToBitset(tline.split(Util.separatorBetweenFields)[0],(dimLookupDict.getDimBitsetLength()+dimLookupDict.getValBitsetLength()),_res2);
                BitSet tdimBitset = tbitset.get(0,dimLookupDict.getDimBitsetLength());
                tdimBitset.xor(dimBitset);
                BitSet tvalBitset = tbitset.get(dimLookupDict.getDimBitsetLength(),(dimLookupDict.getDimBitsetLength()+dimLookupDict.getValBitsetLength()));
                tvalBitset.xor(valBitset);
                if (tdimBitset.cardinality()==2 && tvalBitset.cardinality()==2) {
                    tbitset.or(bitSet);
                    set.add(new ComparableBitSet((BitSet)tbitset.clone()));
                }
                tline = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Override
    public void done() {
        try {
            for (ComparableBitSet comparableBitSet : this.set) {
                String s = comparableBitSet.bitSet.toString();
                this.writer.write(s +"\n");
                this.writer.flush();
            }
            this.writer.close();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public static void main(String[] args) {
        try {
            CandidateGeneratorMapReduce generatorMapReduce = new CandidateGeneratorMapReduce("bitsetRedOp1.tmp","testStageCandidate.tmp","dim_mapping.tmp","dim_val_mapping.tmp");
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
