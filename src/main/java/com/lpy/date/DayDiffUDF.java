package com.lpy.date;

/**
 * @Description todo:
 * @Author xiaoyun
 * @Date 2020-04-20 09:55
 */

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormatter;

/**
 * / **
 *  *给定YYYYMMDD中的两个日期，
 *  *返回之间的天数
 *  *每天午夜..
 * * < p />
 *  * day_diff（“ 20120701”，“ 20120702”）== 1
 *  * day_diff（“ 20120701”，“ 20120701”）== 0
 *  * /
 */
@Description(
        name = "day_diff",
        value = " The difference in days of two YYYYMMDD dates"
)
public class DayDiffUDF extends UDF {

    private static final Logger LOG = Logger.getLogger(DayDiffUDF.class);
    private static final DateTimeFormatter YYYYMMDD = org.joda.time.format.DateTimeFormat.forPattern("YYYYMMdd");

    public Integer evaluate(String date1Str, String date2Str) {
        DateTime dt1 = YYYYMMDD.parseDateTime(date1Str);
        DateTime dt2 = YYYYMMDD.parseDateTime(date2Str);


        int dayDiff = Days.daysBetween(dt1, dt2).getDays();

        return dayDiff;
    }

}
