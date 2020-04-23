package com.lpy.date;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Date;

/**
 * @Description todo:
 * @Author xiaoyun
 * @Date 2020-04-17 15:20
 * 简单的日期添加UDF。
 * 将使用Hive标准功能，但是假设
 * 日期格式为“ YYYY-MM-DD”，我们希望使用“ YYYYMMDD”
 * 并且在我们的配置单元中包含很多子字符串函数太尴尬了
 */
public class AddDaysUDF extends UDF {

    private static final Logger LOGGER = Logger.getLogger(AddDaysUDF.class);
    private static final DateTimeFormatter YYYYMMDD = DateTimeFormat.forPattern("YYYYMMdd");

    public String evaluate(String dateStr,int numDays){
        DateTime dt = YYYYMMDD.parseDateTime(dateStr);
        DateTime addDt = dt.plus(numDays);
        String addedDtStr = YYYYMMDD.print(addDt);
        return addedDtStr;
    }

    public static void main(String[] args) {
        String evaluate = new AddDaysUDF().evaluate("20200417", 12);
        System.out.println(evaluate);
    }


}
