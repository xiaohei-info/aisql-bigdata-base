package org.aisql.bigdata.base.util;

import com.alibaba.fastjson.JSON;

/**
 * Author: xiaohei
 * Date: 2019/10/21
 * Email: xiaohei.info@gmail.com
 * Host: xiaohei.info
 */

/**
 * scala 对象序列化与可变参数重载等解析与java不同,故使用Java调用这部分功能
 */
public class JavaJsonUtil {
    public static String toJSONString(Object obj) {
        return JSON.toJSONString(obj);
    }
}
