package com.lpy.Assert;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;

/**
 * @Description todo:
 * @Author xiaoyun
 * @Date 2020-04-17 18:07
 */
public class AssertEqualsUDF extends UDF {

    private static final Logger LOG = Logger.getLogger(AssertEqualsUDF.class);

    public String evaluate(Double val1,Double val2){
        if (val1 == null || val2 == null){
            LOG.error("NULL values found ::"+val1+"=="+val2);
            System.err.println("Null values found::"+val1+"=="+val2);
            throw new RuntimeException(" Null values found :: " + val1 + " == " + val2);
        }
        if (!val1.equals(val2)) {
            LOG.error(" Assertion Not Met :: ! ( " + val1 + " == " + val2 + " ) ");
            System.err.println(" Assertion Not Met :: ! ( " + val1 + " == " + val2 + " ) ");
            throw new RuntimeException(" Assertion Not Met :: ! ( " + val1 + " == " + val2 + " ) ");
        } else {
            return val1.toString() + " == " + val2.toString();
        }

    }


}
