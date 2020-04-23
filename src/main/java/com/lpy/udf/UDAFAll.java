package com.lpy.udf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * @ClassName: UDAFAll
 * @Author: lipy
 * @Date:
 * @Version : V1.0
 **/

/**
 *
 *  如果传递给该函数的布尔值的所有列均为true，则返回true。
 *
 *  如果任何项目为假，则返回假。
 *  如果没有“ false”，但有null，则我们不知道：返回null。
 *  如果所有行都为true，则返回true。
 *  如果没有行，则0为true，ALL为true。返回空值。
 *
 *  语义上等同于：
 *   NOT ANY（非列）
 *
 */
public class UDAFAll extends UDAF {


    public static class UDAFAllEvaluator implements UDAFEvaluator {

        Boolean result = null;
        Boolean any_rows_seen = false;

        public UDAFAllEvaluator() {
            super();
            init();
        }

        public void init() {
            result = null; // Return null for 0 rows
            any_rows_seen = false;
        }

        public boolean iterate(Boolean ThisBool) {
            if (result != null && !result) {  //达到完成状态。
                ;
            } else if (ThisBool == null) {  //我们不再返回true
                result = null;
            } else if (!ThisBool) {   //确定！我们有答案。.
                result = false;
            } else if (!any_rows_seen) {  // ThisBool现在必须为true
                result = true;  //大于0行的不同初始化状态
            } else {
                ; //维持当前的假设
            }
            any_rows_seen = true;
            return true;
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
