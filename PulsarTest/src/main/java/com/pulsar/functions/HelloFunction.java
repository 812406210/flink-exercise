package com.pulsar.functions;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-05-11 14:41
 */
public class HelloFunction implements Function<String, String> {
    @Override
    public String process(String s, Context context) throws Exception {
        return String.format("hello, %s!", s);
    }
}
