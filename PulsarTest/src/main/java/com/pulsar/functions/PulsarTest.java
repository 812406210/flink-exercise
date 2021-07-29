package com.pulsar.functions;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.functions.LocalRunner;

import java.util.Collections;

/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-05-11 14:42
 */
public class PulsarTest {
    public static void main(String[] args) throws Exception {
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setName("exclamation");
        //从input-topic获取数据
        functionConfig.setInputs(Collections.singleton("my-topic"));
        //写入数据到output-topic
        functionConfig.setOutput("output-topic");
        functionConfig.setClassName(HelloFunction.class.getName());
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        LocalRunner localRunner = LocalRunner.builder()
                .functionConfig(functionConfig)
                .brokerServiceUrl("pulsar://hadoop101:6650")
                .build();
        localRunner.start(true);
    }
}
