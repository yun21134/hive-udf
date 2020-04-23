package com.lpy.Assert;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * @Description todo:
 * @Author xiaoyun
 * @Date 2020-04-20 09:20
 */
/**
 *
 */
@Description(
        name = "assert",
        value = " Asserts in case boolean input is false. Optionally it asserts with message if input string provided. \n " +
                "_FUNC_(boolean) \n" +
                "_FUNC_(boolean, string) "
)
public class AssertUDF extends UDF {

    public String evaluate(Boolean doNotThrowAssertion, String assertionMessage) throws HiveException {
        if (doNotThrowAssertion) {
            return "OK";
        }
        throw (assertionMessage == null) ? new HiveException() : new HiveException(assertionMessage);
    }

    public String evaluate(Boolean doNotThrowAssertion) throws HiveException {
        return evaluate(doNotThrowAssertion, null);
    }

}
