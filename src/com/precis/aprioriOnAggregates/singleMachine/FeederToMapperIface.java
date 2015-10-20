package com.precis.aprioriOnAggregates.singleMachine;


public interface FeederToMapperIface {

   public void evaluate(String line);
   public void done();

}
