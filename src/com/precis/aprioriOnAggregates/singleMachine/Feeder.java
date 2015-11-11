package com.precis.aprioriOnAggregates.singleMachine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.FileReader;
import java.lang.StringBuilder;
import java.util.Arrays;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.nio.charset.Charset;
import java.util.HashSet;



public class Feeder {

        private FeederToMapperIface fMap = null;
        private FeederToReducerIface fRed = null;
        private int numberOfDimensions = 0;
        private int numberOfMetrics    = 0;
        private double threshold = 0;
        private Integer maxNumberOfStages;
        private static List<String> unsortedList = null;
        private static String [] unsortedArray = null;

        public Feeder(String schemaFile, double t, Integer maxNumberOfStages) throws IOException, FileNotFoundException{
               threshold = t;
               fMap = new AnalyserStage1Mapper(schemaFile,Util.mapToSS_Stage1);
               fRed = new AnalyserStage1Reducer(threshold,Util.redOp_Stage1);
               this.maxNumberOfStages = maxNumberOfStages;
        }

        public void startToFeed(BufferedReader br) throws IOException {
            TimeCalculator.getDifference();
            MRStage1(br);
            System.out.println("Stage 1 " + TimeCalculator.getDifference());
            preCandidateGeneration1();
            System.out.println("precandidate generation 1 " + TimeCalculator.getDifference());
            preCandidateGeneration2();
            System.out.println("precandidate generation 2 " + TimeCalculator.getDifference());
            preCandidateGeneration3();
            System.out.println("precandidate generation 3 " + TimeCalculator.getDifference());
            Integer generatedCandidate = 100;
            String lastStageFilename = Util.redopStage1Bitset;
            Integer stage = new Integer(2);
            String candidateFilename = "tmpCandidateFile" + stage.toString() + ".tmp";
            this.generateCandidateWithoutPartition(lastStageFilename, candidateFilename);
            System.out.println("stage " + stage + " candidate generation " + TimeCalculator.getDifference());
            generatedCandidate = this.filterCandidates(candidateFilename, stage);
            System.out.println("stage " + stage + " candidate filteration " + TimeCalculator.getDifference());
            lastStageFilename = stage.toString() + "_stage_string_output.tmp";
            stage = stage + 1;
            while (generatedCandidate > 0 && stage <= this.maxNumberOfStages) {
                candidateFilename = "tmpCandidateFile" + stage.toString() + ".tmp";
                this.generateCandidate(lastStageFilename, candidateFilename,stage);
                System.out.println("stage " + stage + " candidate generation " + TimeCalculator.getDifference());
                generatedCandidate = this.filterCandidates(candidateFilename, stage);
                System.out.println("stage " + stage + " candidate filteration " + TimeCalculator.getDifference());
                lastStageFilename = stage.toString() + "_stage_string_output.tmp";
                stage = stage + 1;
            }
        }

        public void generateCandidate(String lastStageFilename, String candidateFilename, Integer stage) {
            try {
                Partition partition = new Partition(lastStageFilename,stage,Util.dim_mapping,Util.dim_val_mapping);
                String partFile = partition.generatePartitionFile();
                while (partFile!=null) {
                    if (true) {
                        CandidateGeneratorMapReduce generatorMapReduce = new CandidateGeneratorMapReduce(partFile, "unsorted_"+candidateFilename, Util.dim_mapping, Util.dim_val_mapping);
                        BufferedReader reader = new BufferedReader(new FileReader(partFile));
                        String line = reader.readLine();
                        while (line != null) {
                            generatorMapReduce.evaluate(line);
                            line = reader.readLine();
                        }
                        generatorMapReduce.done();
                    }
                    partFile = partition.generatePartitionFile();
                }

                Util.sortFileAIntoFileB("unsorted_" + candidateFilename, candidateFilename);
            } catch (Exception e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }

        public void generateCandidateWithoutPartition(String lastStageFilename, String candidateFilename) {
            try {
                CandidateGeneratorMapReduce generatorMapReduce = new CandidateGeneratorMapReduce(lastStageFilename, candidateFilename, Util.dim_mapping, Util.dim_val_mapping);
                BufferedReader reader = new BufferedReader(new FileReader(lastStageFilename));
                String line = reader.readLine();
                while (line != null) {
                    generatorMapReduce.evaluate(line);
                    line = reader.readLine();
                }
                generatorMapReduce.done();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        public Integer filterCandidates(String candidateFilename, Integer stage) {
            try {
                String stageOutputFilename = stage.toString() + "_stage_string_output.tmp";
                CandidateFilter filter = new CandidateFilter(stage, Util.dim_mapping, Util.dim_val_mapping, Util.basicDataFileBitsetFormat, stageOutputFilename, this.threshold);
                BufferedReader reader = new BufferedReader(new FileReader(candidateFilename));
                String line = reader.readLine();
                while (line != null) {
                    filter.evaluate(line);
                    line = reader.readLine();
                }
                filter.done();
                //BitsetFeedToStringFeedConverter converter = new BitsetFeedToStringFeedConverter((stage.toString() +"_stage_string_output.tmp"), Util.dim_mapping, Util.dim_val_mapping, true);
                //converter.convertFeed(stageOutputFilename);
                return filter.recordsWritten;
            } catch (Exception e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            return 0;
        }


        public void MRStage1 (BufferedReader br) throws IOException {
                String line = null;
                while ( (line = br.readLine()) != null) {
                        fMap.evaluate(line);
                }   
                fMap.done();

                //sort and Shuffle
                BufferedReader ss = new BufferedReader(new FileReader(Util.mapToSS_Stage1));
                line = null;
                ArrayList<String> recs= new ArrayList<String>();
                while ( (line = ss.readLine()) != null) {
                        recs.add(line);
                }
                Object [] obj = recs.toArray(); //toCharArray()
                String[] data = Arrays.copyOf(obj,obj.length,String[].class);

                Arrays.sort(data);
                BufferedWriter bw = new BufferedWriter (new FileWriter(Util.SSToRed_Stage1));
                for (int j = 0; j < data.length; j++) {
                        bw.write(data[j]);
                        bw.newLine();
                        bw.flush();        
                }

                BufferedReader red = new BufferedReader(new FileReader(Util.SSToRed_Stage1));                
                line = null;
                while ( (line = red.readLine()) != null) {
                        fRed.evaluate(line);
                }   
                fRed.done();
        }
        public void preCandidateGeneration1() {
              try {
                      DimLookupDict dimLookup = null;
                      dimLookup = new DimLookupDict(Util.dim_mapping, Util.dim_val_mapping);
        		      BufferedReader readRedOp1 = new BufferedReader(new FileReader(Util.redOp_Stage1));
                      String line = null;
                      while ( (line = readRedOp1.readLine()) != null) {
                              dimLookup.evaluate(line);
                      }
		      dimLookup.done();
              } catch (Exception e) {
                      e.printStackTrace();
              }
        }
	public void preCandidateGeneration2() {
        try {
            BaseFeedConverter baseFeedConverter = new BaseFeedConverter(Util.schemaFile,Util.basicDataFile,Util.basicDataFileBitsetFormat,Util.dim_mapping,Util.dim_val_mapping);
            BufferedReader reader = new BufferedReader(new FileReader(Util.basicDataFile));
            String line = reader.readLine();
            while (line!=null) {
                baseFeedConverter.evaluate(line);
                line = reader.readLine();
            }
            baseFeedConverter.done();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public void preCandidateGeneration3() {
        try {
            StringFeedToBitsetFeedConverter converter = new StringFeedToBitsetFeedConverter(Util.redopStage1Bitset,Util.dim_mapping,Util.dim_val_mapping,true);
            BufferedReader reader = new BufferedReader(new FileReader(Util.redOp_Stage1));
            String line = reader.readLine();
            while (line!=null) {
                converter.evaluate(line);
                line = reader.readLine();
            }
            converter.done();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
