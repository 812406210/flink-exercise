package com.ywj.examples;

import com.ywj.domains.Person;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.drools.core.io.impl.ClassPathResource;
import org.kie.api.KieBase;
import org.kie.api.runtime.StatelessKieSession;
import org.kie.internal.utils.KieHelper;

/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-07-06 18:30
 */
public class TestDrools {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Person> input = env.fromElements(
                new Person("1",8),
                new Person("2",10),
                new Person("3",18),
                new Person("4",15),
                new Person("5",25));
        input.map(new RichMapFunction<Person , Person>() {
            @Override
            public Person map(Person s) throws Exception {

                KieHelper kieHelper = new KieHelper();
//                kieHelper.addContent()
                kieHelper.kfs.write( new ClassPathResource( "rules/Sample.drl"));
                KieBase kbase = kieHelper.build();
                StatelessKieSession statelessKieSession = kbase.newStatelessKieSession();
                statelessKieSession.execute(s);
                return s;
            }
        }).print();

        env.execute("cep");
    }
}
