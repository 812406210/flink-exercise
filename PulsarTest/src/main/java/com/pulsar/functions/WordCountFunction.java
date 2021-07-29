package com.pulsar.functions;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import java.util.Arrays;

/**
 * @program: FlinkSql
 * @description: wc
 * @author: yang
 * @create: 2021-05-11 14:34
 */
public class WordCountFunction implements Function<String,Void> {

    @Override
    public Void process(String s, Context context) throws Exception {
        Arrays.asList(s.split(",")).forEach(word ->{
            String counterKey = word.toLowerCase();
            context.incrCounter(counterKey,1);
        });
        return null;
    }
}
