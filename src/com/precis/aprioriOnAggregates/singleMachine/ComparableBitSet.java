package com.precis.aprioriOnAggregates.singleMachine;

import java.util.BitSet;

/**
 * Created with IntelliJ IDEA.
 * User: nitinkau
 * Date: 6/25/14
 * Time: 5:45 PM
 * To change this template use File | Settings | File Templates.
 */
public class ComparableBitSet implements Comparable<ComparableBitSet> {
    public BitSet bitSet;

    public ComparableBitSet(BitSet bitSet) {
        this.bitSet = bitSet;
    }

    @Override
    public int compareTo(ComparableBitSet o) {
        Integer bitsetIndex1 = this.bitSet.nextSetBit(0);
        Integer bitsetIndex2 = o.bitSet.nextSetBit(0);
        while (bitsetIndex1==bitsetIndex2 && bitsetIndex1!=-1) {
            bitsetIndex1 = this.bitSet.nextSetBit(bitsetIndex1+1);
            bitsetIndex2 = o.bitSet.nextSetBit(bitsetIndex2+1);
        }
        return bitsetIndex1-bitsetIndex2;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public String toString() {
        return this.bitSet.toString();
    }
}
