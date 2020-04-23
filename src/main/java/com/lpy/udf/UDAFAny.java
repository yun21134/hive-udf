package com.lpy.udf;

/**
 * @ClassName: UDAFAny
 * @Author: lipy
 * @Date:
 * @Version : V1.0
 **/

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 *
 *  如果传递给该函数的布尔值列的ANY为true，则返回true：
 *
 *  如果任何项目为“ true”，则返回true。
 *  如果没有“ true”，但有空值，则我们不知道：返回空值。
 *  如果所有行均为false，则返回false。
 *  如果没有行，则没有真项，则返回假。
 *
 *  语义上等同于：
 *  不是全部（“非”列）
 *
 */
public class UDAFAny extends UDAF {


    public static class UDAFAnyEvaluator implements UDAFEvaluator {

        Boolean result = false;

        public UDAFAnyEvaluator() {
            super();
            init();
        }

        public void init() {
            result = false;
        }

        public boolean iterate(Boolean ThisBool) {
            if (result != null && result) {  //达到完成状态。
                return true;
            } else if (ThisBool == null) { //我们不再可以返回false。
                result = null;
                return true;
            } else if (ThisBool) {  //成功！
                result = true;
                return true;
            } else {
                return true;
            }
        }

        public Boolean terminatePartial() {
            return result;
        }

        public boolean merge(Boolean soFar) {
            iterate(soFar);
            return true;
        }

        public Boolean terminate() {
            return result;
        }
    }


}
