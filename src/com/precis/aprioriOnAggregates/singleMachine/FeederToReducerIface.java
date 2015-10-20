package com.precis.aprioriOnAggregates.singleMachine;


public interface FeederToReducerIface {

   public void evaluate(String line);
   public void done();

}
