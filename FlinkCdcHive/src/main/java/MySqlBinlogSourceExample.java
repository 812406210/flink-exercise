import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-06-18 15:38
 */
public class MySqlBinlogSourceExample {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        //todo 1、读取mysqlbinlog
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SourceFunction<JSONObject> sourceFunction = MySQLSource.<JSONObject>builder()
                .hostname("hadoop101")
                .port(3306)
                .databaseList("test")
                .username("root")
                .password("yang156122")
                // todo 2、读取数据解析为json数据
                //数据样例： {"canal_type":"insert","create_time":1622815639000,"name":"user0","id":1,"canal_ts":0,"age":1.0,"canal_database":"user","pk_hashcode":1}
                .deserializer(new CdcDeserializationSchema())
                .build();
        DataStreamSource<JSONObject> jsonDataStreamSource = env.addSource(sourceFunction);

        //todo 3、过滤数据
        SingleOutputStreamOperator<JSONObject> userData = jsonDataStreamSource.filter(data -> {
            if (("user".equals(data.getString("canal_database") ))
                && !("detele".equals(data.getString("canal_type"))))
            { return true; } else { return false; }
        });

        //todo 4、只拿业务数据
        String strDateFormat = "yyyy-MM-dd HH:mm:ss";
        SimpleDateFormat dateFormat = new SimpleDateFormat(strDateFormat);
        SingleOutputStreamOperator<String> resultData = userData.map(data -> {
            //时间处理
//            String create_time = data.getString("create_time");
//            data.put("create_time",dateFormat.format(new Date(Long.valueOf(create_time) - 28800000)));
            data.remove("canal_database");
            data.remove("canal_type");
            data.remove("canal_ts");
            data.remove("pk_hashcode");
            return data.toJSONString();
        });
        //todo 5、sink操作
        //5.1 kafka sink  uat-datacenter1
        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>("hadoop103:9092",
                "user_changelog_json", new SimpleStringSchema());
        resultData.addSink(kafkaSink);

        //5.2 打印数据
        resultData.print("最终数据===>");

        try {
            env.execute("测试mysql-cdc");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}



class CdcDeserializationSchema implements DebeziumDeserializationSchema<JSONObject> {
    private static final long serialVersionUID = -3168848963265670603L;

    public CdcDeserializationSchema() {
    }

    @Override
    public void deserialize(SourceRecord record, Collector<JSONObject> out) throws Exception {
        Struct dataRecord  =  (Struct)record.value();

        Struct afterStruct = dataRecord.getStruct("after");
        Struct beforeStruct = dataRecord.getStruct("before");
        /*
          todo 1，同时存在 beforeStruct 跟 afterStruct数据的话，就代表是update的数据
               2,只存在 beforeStruct 就是delete数据
               3，只存在 afterStruct数据 就是insert数据
         */
        JSONObject logJson = new JSONObject();

        String canal_type = "";
        List<Field> fieldsList = null;
        if(afterStruct !=null && beforeStruct !=null){
            System.out.println("这是修改数据");
            canal_type = "update";
            fieldsList = afterStruct.schema().fields();
            //todo 字段与值
            for (Field field : fieldsList) {
                String fieldName = field.name();
                Object fieldValue = afterStruct.get(fieldName);
                logJson.put(fieldName,fieldValue);
            }
        }else if (afterStruct !=null){
            System.out.println( "这是新增数据");

            canal_type = "insert";
            fieldsList = afterStruct.schema().fields();
            //todo 字段与值
            for (Field field : fieldsList) {
                String fieldName = field.name();
                Object fieldValue = afterStruct.get(fieldName);
                logJson.put(fieldName,fieldValue);
            }
        }else if (beforeStruct !=null){
            System.out.println( "这是删除数据");
            canal_type = "detele";
            fieldsList = beforeStruct.schema().fields();
            //todo 字段与值
            for (Field field : fieldsList) {
                String fieldName = field.name();
                Object fieldValue = beforeStruct.get(fieldName);
                logJson.put(fieldName,fieldValue);
            }
        }else {
            System.out.println("一脸蒙蔽了");
        }

        //todo 拿到databases table信息
        Struct source = dataRecord.getStruct("source");
        Object db = source.get("db");
        Object table = source.get("table");
        Object ts_ms = source.get("ts_ms");

        logJson.put("canal_database",db);
        logJson.put("canal_database",table);
        logJson.put("canal_ts",ts_ms);
        logJson.put("canal_type",canal_type);

        //todo 拿到topic
        String topic = record.topic();
        System.out.println("topic = " + topic);

        //todo 主键字段
        Struct pk = (Struct)record.key();
        List<Field> pkFieldList = pk.schema().fields();
        int partitionerNum = 0 ;
        for (Field field : pkFieldList) {
            Object pkValue= pk.get(field.name());
            partitionerNum += pkValue.hashCode();

        }
        int hash = Math.abs(partitionerNum) % 3;
        logJson.put("pk_hashcode",hash);
        out.collect(logJson);
    }
    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return BasicTypeInfo.of(JSONObject.class);
    }
}


