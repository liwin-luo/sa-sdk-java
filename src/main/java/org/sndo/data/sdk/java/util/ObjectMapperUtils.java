package org.sndo.data.sdk.java.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;

import java.text.SimpleDateFormat;

public class ObjectMapperUtils {

    public static ObjectMapper getJsonObjectMapper() {
        ObjectMapper jsonObjectMapper = new ObjectMapper();
        // 容忍json中出现未知的列
        jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // 兼容java中的驼峰的字段名命名
        jsonObjectMapper.setPropertyNamingStrategy(
                PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
        jsonObjectMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));
        return jsonObjectMapper;
    }

}
