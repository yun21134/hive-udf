package com.lpy.Assert;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;

/**
 * @Description todo:
 * @Author xiaoyun
 * @Date 2020-04-20 09:21
 */
public class ThrowErrorUDF extends UDF {

    private static final Logger LOG = Logger.getLogger(ThrowErrorUDF.class);


    public String evaluate(String errorMessage) {
        LOG.error("Assertion not met :: " + errorMessage);
        System.err.println("Assertion not met :: " + errorMessage);

        throw new RuntimeException(errorMessage);
    }

}
