import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.canal.util.HBaseUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;

import java.time.Duration;
import java.util.*;

/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-07-01 10:05
 */
public class ManyKafka2HbaseAdapter {

    final static Logger logger = LoggerFactory.getLogger(ManyKafka2HbaseAdapter.class);
    static Map  hbaseMap = new HashMap();
    static Properties p = new Properties();
    static {

        InputStream in = ManyKafka2HbaseAdapter.class.getResourceAsStream("parameters.properties");
        try {
            p.load(in);
            //hbase 表信息
            String tableName = p.getProperty("hbase.table.name");
            String[] tableNames = tableName.split(",");
            String familyName = p.getProperty("hbase.table.demo.familyName");
            String[] familyNames = familyName.split(",");
            //mysql 表信息
            String mysqlTableName = p.getProperty("mysql.table.name");
            String[] mysqlTableNames = mysqlTableName.split(",");

            for (int i = 0; i < tableNames.length; i++) {
                Map  tableMap  = new HashMap();
                tableMap.put("tableName",tableNames[i]);
                tableMap.put("familyName",familyNames[i]);
                hbaseMap.put(mysqlTableNames[i],tableMap);
            }
        } catch (IOException e) {
            logger.info("解析配置文件失败........,error={}",e.getMessage());
        }
    }


    public static void main(String[] args) {
        //kafka 初始化
        KafkaConsumer<String, String> kafkaConsumer = initKafka();

        while (true) {

            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                JSONObject recordJson = new JSONObject();
                Map map = new HashMap();
                try {
                    recordJson = JSON.parseObject(record.value());
                    JSONObject dbData = recordJson.getJSONArray("data").getJSONObject(0);
                    map = JSON.parseObject(String.valueOf(dbData), Map.class);
                }catch (Exception e){
                    logger.info("解析失败....,record={}",record.value());
                }
                // mysql 表名
                String table = recordJson.getString("table");
                if("UPDATE".equals(recordJson.getString("type"))){
                    //更新
                    try {
                       operate(table,map);
                    } catch (Exception e) {
                        logger.info("入库hbase error....,error={}",e.getMessage());
                    }
                }else if("INSERT".equals(recordJson.getString("type"))){
                    //增加
                    try {
                      operate(table,map);
                    } catch (Exception e) {
                        logger.info("更新hbase error....,error={}",e.getMessage());
                    }
                }else {
                    //删除 DELETE
                    try {
                        Map<String,String> infoMap = (HashMap)hbaseMap.get(table);
                        String rowKey = table+(String) map.get("id");
                        HBaseUtil.deleteByKey(infoMap.get("tableName"),rowKey);
                    } catch (Exception e) {
                        logger.info("删除hbase error....,error={}",e.getMessage());
                    }
                }

                System.out.println(String.format("topic:%s,offset:%d,partition:%d,消息:%s", record.topic(), record.offset(),record.partition(), record.value()));

            }
        }
    }

    /**
     * 如果需要写入其他的hbase,需要在这里加判断逻辑
     * @param table  mysql表名
     * @param map  hbase表信息
     * @throws Exception
     */
    public static void operate(String table,Map map) throws Exception{
        Map<String,String> infoMap = (HashMap)hbaseMap.get(table);
        String rowKey = table+map.get("id");
        HBaseUtil.put(infoMap.get("tableName"),rowKey,infoMap.get("familyName"),map);
        //mysql 表名判断 ,roekey规则设计可以放开这段
//        if("user".equals(table)){
//            // 根据不同表规则组建rowkey
//            String rowKey = (String) map.get("id");
//            HBaseUtils.put(infoMap.get("tableName"),rowKey,infoMap.get("familyName"),map);
//        }else if("user_info".equals(table)){
//            String rowKey = (String) map.get("id");
//            HBaseUtils.put(infoMap.get("tableName"),rowKey,infoMap.get("familyName"),map);
//        }
    }

    public static KafkaConsumer<String, String>  initKafka(){
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, p.get("bootstrap.servers"));
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, p.getProperty("group.id"));
        //注意，指定了offset消费，这个配置无效
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, p.get("auto.offset.reset"));
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(p);
        //从offset消息，解除注释。 订阅消息: 如果第一次启动则使用 kafkaConsumer.subscribe; 否则使用getKafkaOffset()

       kafkaConsumer.subscribe(Arrays.asList(p.getProperty("kafka.topics").split(",")));
        return kafkaConsumer;
    }




}
