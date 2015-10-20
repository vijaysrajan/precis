package com.precis.aprioriOnAggregates.singleMachine;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class DimLookupDict  implements FeederToMapperIface  {
    private static DimLookupDict dimLookupDict = null;
    private Map<Integer,String> bitToDim;
    private Map<Integer,Map<String,String>> bitToVal;
    private Map<String,Integer> dimToBit;
    private Map<String,Map<String,Integer>> valToBit;
    private static String inputFile = null;
    private static String dimMappingFile = null;
    private static String valMappingFile = null;
    private Integer dimBitsetLength;
    private Integer valBitsetLength;
    private BufferedWriter bwDimMapping = null;
    private BufferedWriter bwDimValMapping = null;

    public Integer getDimBitsetLength() {
        return dimBitsetLength;
    }

    public Integer getValBitsetLength() {
        return valBitsetLength;
    }

    private DimLookupDict() {
        this.initHashMaps();
    }
    private void initHashMaps() {
        this.bitToDim = new HashMap<Integer, String>();
        this.bitToVal = new HashMap<Integer, Map<String, String>>();
        this.dimToBit = new HashMap<String, Integer>();
        this.valToBit = new HashMap<String, Map<String, Integer>>();
    }   

    public DimLookupDict(String fDimMapping, String fDimValMapping)  throws IOException, FileNotFoundException {
        bwDimMapping = new BufferedWriter (new FileWriter(fDimMapping));        
        bwDimValMapping = new BufferedWriter (new FileWriter(fDimValMapping));        
        this.initHashMaps();
    }
    private Integer dimIndex=0;
    private Integer valIndex=0;
    public void evaluate (String line) {
           try {

                   String[] cols = line.split(String.valueOf(Util.separatorBetweenFields));
                   String dim = cols[0].split(String.valueOf(Util.separatorBetweenDimAndVal))[0];
                   String val = cols[0].split(String.valueOf(Util.separatorBetweenDimAndVal))[1];
                   if (dimToBit.get(dim)==null) {
                       dimToBit.put(dim,dimIndex);
                       bitToDim.put(dimIndex,dim);
                       valToBit.put(dim,new HashMap<String, Integer>());
                       bwDimMapping.write(dimIndex.toString() );
                       bwDimMapping.write(String.valueOf(Util.separatorBetweenFields));
                       bwDimMapping.write(dim);
                       bwDimMapping.write("\n");
                       dimIndex++;
                   }
                   if (valToBit.get(dim).get(val)==null) {
                       valToBit.get(dim).put(val,valIndex);
                       Map<String,String> pair = new HashMap<String, String>();
                       pair.put(dim,val);
                       bitToVal.put(valIndex,pair);
                       bwDimValMapping.write(valIndex.toString());
                       bwDimValMapping.write(String.valueOf(Util.separatorBetweenFields));
                       bwDimValMapping.write(dim);
                       bwDimValMapping.write(Util.separatorBetweenDimAndVal);
                       bwDimValMapping.write(val);
                       bwDimValMapping.write("\n");
                       valIndex++;
                   }
         } catch (Exception e) {
                     e.printStackTrace();
         }
    }

    public void done() {
         try {
             bwDimMapping.flush();
             bwDimValMapping.flush();
         } catch (Exception e) {
                     e.printStackTrace();
         }
    }


    private void doMapping() throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        String line = reader.readLine();
        while (line!=null) {
            String[] cols = line.split(String.valueOf(Util.separatorBetweenFields));
            String dim = cols[0].split(String.valueOf(Util.separatorBetweenDimAndVal))[0];
            String val = cols[0].split(String.valueOf(Util.separatorBetweenDimAndVal))[1];
            if (dimToBit.get(dim)==null) {
                dimToBit.put(dim,dimIndex);
                bitToDim.put(dimIndex,dim);
                valToBit.put(dim,new HashMap<String, Integer>());
                dimIndex++;
            }
            if (valToBit.get(dim).get(val)==null) {
                valToBit.get(dim).put(val,valIndex);
                Map<String,String> pair = new HashMap<String, String>();
                pair.put(dim,val);
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
            writer.write(entry.getKey()+String.valueOf(Util.separatorBetweenFields)+entry.getValue()+"\n");
        }
        writer.close();

        File valMappingFile = new File(this.valMappingFile);
        if (!valMappingFile.exists()) {
            valMappingFile.createNewFile();
        }
        writer = new BufferedWriter(new FileWriter(valMappingFile));
        for (Map.Entry<Integer,Map<String,String>> entry : this.bitToVal.entrySet()) {
            for (Map.Entry<String,String> entry1 : entry.getValue().entrySet())
            writer.write(entry.getKey()+String.valueOf(Util.separatorBetweenFields)+entry1.getKey()+String.valueOf(Util.separatorBetweenDimAndVal)+entry1.getValue()+"\n");
        }
        writer.close();
        this.dimBitsetLength = this.bitToDim.size();
        this.valBitsetLength = this.bitToVal.size();
    }

    public static void setInputFile(String inputFile) {
        DimLookupDict.inputFile = inputFile;
    }

    public static void setDimMappingFile(String dimMappingFile) {
        DimLookupDict.dimMappingFile = dimMappingFile;
    }

    public static void setValMappingFile(String valMappingFile) {
        DimLookupDict.valMappingFile = valMappingFile;
    }

    public Integer getBitIndexForDim(String dim) {
        return this.dimToBit.get(dim);
    }

    public String getDimForBitIndex(Integer bitIndex) {
        return this.bitToDim.get(bitIndex);
    }

    public Integer getBitIndexForDimValue(String dim, String val) {
        if (this.valToBit.get(dim)==null) {
            return null;
        }
        return this.valToBit.get(dim).get(val);
    }

    public Map<String,String> getDimValueForBitINdex(Integer bitIndex) {
        return this.bitToVal.get(bitIndex);
    }

    public static DimLookupDict getInstanceForInputfile() throws FileNotFoundException {
        if (dimLookupDict==null) {
            if (DimLookupDict.inputFile==null || DimLookupDict.valMappingFile==null||DimLookupDict.dimMappingFile==null) {
                throw new FileNotFoundException();
            }
            dimLookupDict = new  DimLookupDict();
            try {
                dimLookupDict.doMapping();
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
        return dimLookupDict;
    }

    public static DimLookupDict getInstanceFromMappingFiles() throws IOException {
        if (dimLookupDict==null) {
            if (DimLookupDict.dimMappingFile==null || DimLookupDict.valMappingFile==null) {
                throw new FileNotFoundException();
            }
            dimLookupDict = new DimLookupDict();
            //dimLookupDict.reverseMapping();
        }
        return dimLookupDict;
    }

	public static DimLookupDict generateMapFromMappingFiles(String dimMappingFilename, String dimValMappingFilename) throws Exception {
        DimLookupDict dimLookupDict = new DimLookupDict();
		BufferedReader reader = new BufferedReader(new FileReader(dimMappingFilename));
		String line = reader.readLine();
		while (line!=null) {
			String dim = line.split(Util.separatorBetweenFields)[1];
			Integer index = Integer.parseInt(line.split(Util.separatorBetweenFields)[0]);
            dimLookupDict.bitToDim.put(index,dim);
            dimLookupDict.dimToBit.put(dim,index);
			line = reader.readLine();
		}
        reader.close();
		reader = new BufferedReader(new FileReader(dimValMappingFilename));
        line = reader.readLine();
        while (line!=null) {
            Integer index = Integer.parseInt(line.split(Util.separatorBetweenFields)[0]);
            String dim = line.split(Util.separatorBetweenFields)[1].split(Util.separatorBetweenDimAndVal)[0];
            String val = line.split(Util.separatorBetweenFields)[1].split(Util.separatorBetweenDimAndVal)[1];
            Map<String,String> map = new HashMap<String, String>();
            map.put(dim,val);
            dimLookupDict.bitToVal.put(index,map);
            if (dimLookupDict.valToBit.get(dim)==null) {
                dimLookupDict.valToBit.put(dim,new HashMap<String, Integer>());
            }
            dimLookupDict.valToBit.get(dim).put(val,index);
            line = reader.readLine();
        }
        reader.close();
        dimLookupDict.dimBitsetLength = dimLookupDict.bitToDim.size();
        dimLookupDict.valBitsetLength = dimLookupDict.bitToVal.size();
        return dimLookupDict;
	}

    public static void main(String[] args) {
        try {
            DimLookupDict dimLookupDict = DimLookupDict.generateMapFromMappingFiles("dim_mapping.tmp","dim_val_mapping.tmp");

        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}

