package com.lpy.date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.joda.time.DateTime;
import org.joda.time.DurationFieldType;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description todo:
 * @Author xiaoyun
 * @Date
 */

/**
 *
 *  DateRange是UDTF，用于生成从开始日期到结束日期的天数，
 *  包含在内。
 * < p />
 *  这可能在以下情况下有用：
 *   范围 ...
 * < p />
 *  从（选择ID，选择t1.id，t1.date，合并（t2.val，0.0）作为val
 *  从tab1侧视图查看rng.date（date_range（tab1.start_date，tab1.end_date）
 * ）rng作为日期，索引）t1左外连接（从tab2中选择val）t2在（
 *   t1.id = t2.id和t1.date = t2.date）;
 *
 */
@Description(name = "date_range", value = "_FUNC_（a，b，c）-生成从a到b递增c的整数范围"
        +  "或将地图元素分成多行和多列")
public class DateRangeUDTF extends GenericUDTF {

    private StringObjectInspector startInspector = null;
    private StringObjectInspector endInspector = null;
    private IntObjectInspector incrInspector = null;
    private StringObjectInspector durationTypeInspector = null;
    private StringObjectInspector dateFormatInspector = null;


    // plus（timeUnit，incr）
    //在Java 8中，我们可以使用ChronoUnit，但我不想添加该依赖项
    //但
    static Map<String, DurationFieldType> durationTypes = new HashMap<String, DurationFieldType>();
    static {
        durationTypes.put("millis", DurationFieldType.millis());
        durationTypes.put("seconds", DurationFieldType.seconds());
        durationTypes.put("minutes", DurationFieldType.minutes());
        durationTypes.put("hours", DurationFieldType.hours());
        durationTypes.put("days", DurationFieldType.days());
        durationTypes.put("months", DurationFieldType.months());
        durationTypes.put("years", DurationFieldType.years());
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {

        String usage = "DateRange takes <startdate>, <enddate>, <optional increment>, <optional durationtype>, <optional dateformat>";

        if ((argOIs.length < 2) || (argOIs.length > 5)) {
            throw new UDFArgumentException(usage);
        }

        if (!(argOIs[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentException(usage);
        } else {
            this.startInspector = (StringObjectInspector) argOIs[0];
        }

        if (!(argOIs[1] instanceof StringObjectInspector)) {
            throw new UDFArgumentException(usage);
        } else {
            this.endInspector = (StringObjectInspector) argOIs[1];
        }

        if (argOIs.length >= 3) {
            if (!(argOIs[2] instanceof IntObjectInspector)) {
                throw new UDFArgumentException(usage);
            } else {
                this.incrInspector = (IntObjectInspector) argOIs[2];
            }
        }

        if (argOIs.length >= 4) {
            if (!(argOIs[3] instanceof StringObjectInspector)) {
                throw new UDFArgumentException(usage);
            } else {
                this.durationTypeInspector = (StringObjectInspector) argOIs[3];
            }
        }

        if (argOIs.length >= 5) {
            if (!(argOIs[4] instanceof StringObjectInspector)) {
                throw new UDFArgumentException(usage);
            } else {
                this.dateFormatInspector = (StringObjectInspector) argOIs[4];
            }
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        fieldNames.add("date");
        fieldNames.add("index");
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    private final Object[] forwardListObj = new Object[2];

    @Override
    public void process(Object[] args) throws HiveException {
        String start = null;
        String end = null;
        int incr = 1;
        DurationFieldType durationType = DurationFieldType.days();
        DateTimeFormatter dateFormatter = org.joda.time.format.DateTimeFormat.forPattern("YYYYMMdd");

        if (args.length >= 2) {
            start = this.startInspector.getPrimitiveJavaObject(args[0]);
            end = this.endInspector.getPrimitiveJavaObject(args[1]);
        }

        if (args.length >= 3) {
            incr = this.incrInspector.get(args[2]);
        }

        if (args.length >= 4) {
            String value = this.durationTypeInspector.getPrimitiveJavaObject(args[3]);
            if ((value != null) && durationTypes.containsKey(value.toLowerCase())) {
                durationType = durationTypes.get(value.toLowerCase());
            }
        }

        if (args.length >= 5) {
            dateFormatter = org.joda.time.format.DateTimeFormat.forPattern(this.dateFormatInspector
                    .getPrimitiveJavaObject(args[4]));
        }

        try {
            DateTime startDt = dateFormatter.parseDateTime(start);
            DateTime endDt = dateFormatter.parseDateTime(end);
            int i = 0;
            for (DateTime dt = startDt; dt.isBefore(endDt) || dt.isEqual(endDt); dt = dt.withFieldAdded(
                    durationType, incr), i++) {
                this.forwardListObj[0] = dateFormatter.print(dt);
                this.forwardListObj[1] = new Integer(i);

                this.forward(this.forwardListObj);
            }
        } catch (IllegalArgumentException badFormat) {
            throw new HiveException("Unable to parse dates; start = " + start + " ; end = " + end);
        }
    }

    @Override
    public void close() throws HiveException {

    }

}
