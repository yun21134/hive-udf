package com.lpy.json;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @Description todo:
 * @Author xiaoyun
 * @Date 2020-04-16 10:29
 */
public class getJsonObj extends UDF {

    public String evaluate(String line, String key) throws JSONException {
        // 1 处理line   服务器时间 | json
        String[] log = line.split("\\|");
        //2 合法性校验
        if (log.length != 2 || StringUtils.isBlank(log[1])) {
            return "";
        }
        // 3 开始处理json
        JSONObject baseJson = new JSONObject(log[1].trim());
        String result = "";
        // 4 根据传进来的key查找相应的value
        if ("et".equals(key)) {
            if (baseJson.has("et")) {
                result = baseJson.getString("et");
            }
        } else if ("st".equals(key)) {
            result = log[0].trim();
        } else {
            JSONObject cm = baseJson.getJSONObject("cm");
            if (cm.has(key)) {
                result = cm.getString(key);
            }
        }
        return result;
    }

}
