package com.precis.aprioriOnAggregates.singleMachine;

import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: nitinkau
 * Date: 8/3/14
 * Time: 11:21 AM
 * To change this template use File | Settings | File Templates.
 */
public class TimeCalculator {
    static Long time=null;
    public static Long getDifference() {
        if (TimeCalculator.time==null) {
            TimeCalculator.time = new Date().getTime();
            return Long.parseLong("0");
        }
        Long curTime = new Date().getTime();
        Long dif = curTime - TimeCalculator.time;
        TimeCalculator.time = curTime;
        return dif;
    }
}
