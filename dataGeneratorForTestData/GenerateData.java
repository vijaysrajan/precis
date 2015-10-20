import java.io.File;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

public class GenerateData {
    public double powerLaw(double a, double k, double x){ // a=any const, k = shd by any -ve constant, x is row number
        double y=0;
        y = a*Math.pow(x, k);
        return y;
    }

    public double curvedPowerLaw(double a, double k, double x){ // same as power law
        double y=0;
        y = Math.pow(x, (a+k*x));
        return y;
    }

    public double brokenPowerLaw(double a, double k1,double k2,double xn, double x){ // k1 ans k2 negative powers , xn is the limit row to change the curve and x is the row number
        double y=0;
        if(x<xn){
            y = a*Math.pow(x, k1);
        }else{
            y = a*Math.pow(xn, k2-k1)*Math.pow(x,k2);
        }

        return y;
    }

    public static void main(String[] args) {
        GenerateData generateData = new GenerateData();
        Integer numberOfDimensions = Integer.parseInt(args[0]);
        Integer minCardinality = Integer.parseInt(args[1]);
        Integer maxCardinality = Integer.parseInt(args[2]);
        Integer numberOfRows = Integer.parseInt(args[3]);
        String fileLocation = args[4];
        String random_method = args[5];
        String metricPowerLaw = args[6];
        Random randomGenerator = new Random();
        Random randomCardinality = new Random();
        Vector<Integer> dimensionsCardinality = new Vector<Integer>();
        for (int i=0; i<numberOfDimensions; i++) {
            int cardinalityDifference = maxCardinality - minCardinality;
            Integer randomNumber = randomCardinality.nextInt(cardinalityDifference+1);
            Integer cardinality = minCardinality + randomNumber;
            dimensionsCardinality.add(cardinality);
        }
        File writeFile = new File(fileLocation);
        BufferedWriter writer = null;
        if (!writeFile.exists()) {
            try {
                writeFile.createNewFile();
            } catch (IOException e) {
                System.out.println("File name not specified");
                return;
            }
        }
        try {
            writer = new BufferedWriter(new FileWriter(writeFile));
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        Set<String> stringSet = new HashSet<String>();
        while (stringSet.size()<numberOfRows) {
            String res = "";
            for (int j=0; j<numberOfDimensions; j++) {
                Integer cardinality = dimensionsCardinality.get(j);
                Integer randomNumber = null;
                if (random_method!=null ) {
                    if (random_method.equalsIgnoreCase("uniform")) {
                        randomNumber = randomGenerator.nextInt(cardinality+1);
                    } else {
                        Double normal = null;
                        while (normal==null || normal>1 || normal<-1) {
                            normal = randomGenerator.nextGaussian();
                        }
                        normal = normal + 1.0;
                        normal = normal/2;
                        normal = normal*cardinality;
                        randomNumber = normal.intValue();
                    }
                }
                res = res + randomNumber.toString();
                res = res + "";
            }
            stringSet.add(res);
        }
        Integer index=1;
        for (String s : stringSet) {
            Double metricValue = null;
            Double x = index.doubleValue();
            if ("normal".equalsIgnoreCase(metricPowerLaw)) {
                metricValue = generateData.powerLaw(23231.0,-1.05,x);
            } else if ("curved".equalsIgnoreCase(metricPowerLaw)) {
                metricValue = generateData.curvedPowerLaw(23231.0,-1.05,x);
            } else {
                Double xn = numberOfRows.doubleValue();
                xn = xn/4;
                metricValue = generateData.brokenPowerLaw(23231.0,-1.02,-1.05,xn,x);
            }
            s = s + metricValue.toString();
	    index++;
            try {
                writer.write(s);
                writer.write("\n");
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                return;
            }
        }
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

}


