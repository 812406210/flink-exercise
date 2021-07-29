package com.canal.app.kafkapartitions;

import javafx.collections.ArrayChangeListener;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-07-29 11:35
 */
public class TestConsumerManyPartition1 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度,为了方便测试，查看消息的顺序，这里设置为1，可以更改为多并行度
        env.setParallelism(1);
        //checkpoint的设置
        //每隔10s进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(30000);
        //设置模式为：exactly_one，仅一次语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //确保检查点之间有1s的时间间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        //检查点必须在10s之内完成，或者被丢弃【checkpoint超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        //同一时间只允许进行一次检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //表示一旦Flink程序被cancel后，会保留checkpoint数据，以便根据实际需要恢复到指定的checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置statebackend,将检查点保存在hdfs上面，默认保存在内存中。这里先保存到本地
        env.setStateBackend(new FsStateBackend("file:///D:/study_workspace/flink_demo/flink-java/StateBackEnd2"));

        //设置kafka消费参数; kafka直接发送单个的字符串即可。
        DataStreamSource<String> dataStreamSource = env.addSource(new MyKafkasource());
        dataStreamSource.print();

        env.execute("kafkaeagle2222");

    }
}


class MyKafkasource implements SourceFunction<String> {

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop101:9092,hadoop102:9092,hadoop103:9092");
        properties.setProperty("zookeeper.connect", "hadoop101:2181,hadoop102:2181,hadoop103:2181");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "console-consumer-7");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty("auto.commit.interval.ms", "1000");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        //方式零 直接全部分区消费
        //kafkaConsumer.subscribe(Arrays.asList("kafkaeagle1"));

        //方式一 读取指定的分区指定的offset,然后消费全部分区数据
//        kafkaConsumer.subscribe(Arrays.asList("kafkaeagle1"));
//        ConsumerRecords<String, String> recordTemp = kafkaConsumer.poll(1000);
//        System.out.println(recordTemp.isEmpty());
//        kafkaConsumer.seek(new TopicPartition("kafkaeagle1", 2),1L);

        //方式二 读取指定的分区指定的offset,然后消费全部分区数据
//        Map<TopicPartition, OffsetAndMetadata> hashMaps = new HashMap<TopicPartition, OffsetAndMetadata>();
//        hashMaps.put(new TopicPartition("kafkaeagle1", 0), new OffsetAndMetadata(0));
//        kafkaConsumer.commitSync(hashMaps);
//        kafkaConsumer.subscribe(Arrays.asList("kafkaeagle1"));

        //方式三 消费指定分区数据以及offset
        kafkaConsumer.assign(Arrays.asList(new TopicPartition("kafkaeagle1", 0)
                ,new TopicPartition("kafkaeagle1", 2)));
        kafkaConsumer.seek(new TopicPartition("kafkaeagle1", 0),2L);
        kafkaConsumer.seek(new TopicPartition("kafkaeagle1", 2),2L);

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value() + " 分区：" + record.partition() + " 偏移量:" + record.offset());
                ctx.collect(record.value());

            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
