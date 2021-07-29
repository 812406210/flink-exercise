import com.alibaba.druid.util.JdbcUtils;

import java.sql.SQLException;
import java.util.*;

/**
 * @program: Flink12Doris
 * @description:
 * @author: yang
 * @create: 2021-06-29 13:47
 */
public class QueryKafkaTopicInfo {

    public static void main(String[] args) throws SQLException {
       // queryData("instance_canal_kafka_test",2);
        //insertData("instance_canal_kafka_test",31,0);
        updateData("instance_canal_kafka_test",14,0);
    }

    public static Map<String, Object>  queryData(String topicName,int partitions) throws SQLException {
        String querySql = "select * from kafka_info where topic_name = ? and partitions = ?";
        List<Map<String, Object>> mapList = JdbcUtils.executeQuery(Objects.requireNonNull(DataSourceConfig.getInstance()), querySql, topicName,partitions);
        return mapList.isEmpty()?null:mapList.get(0);
    }


    public static Map<String, Object>  queryDataByName(String topicName) throws SQLException {
        String querySql = "select * from kafka_info where topic_name = ?";
        List<Map<String, Object>> mapList = JdbcUtils.executeQuery(Objects.requireNonNull(DataSourceConfig.getInstance()), querySql, topicName);
        return mapList.isEmpty()?null:mapList.get(0);
    }


    public static void  insertData(String topicName,long offset,int partitions) throws SQLException {
        String querySql = "insert into kafka_info values(?,?,?)";
        JdbcUtils.executeUpdate(Objects.requireNonNull(DataSourceConfig.getInstance()), querySql, topicName,offset,partitions);

    }

    public static void  updateData(String topicName,long offset,int partitions) throws SQLException {
        String querySql = "UPDATE `test`.`kafka_info` SET  `offset` = ?, `partitions` = ? WHERE `topic_name` = ?";
        JdbcUtils.executeUpdate(Objects.requireNonNull(DataSourceConfig.getInstance()), querySql,offset,partitions,topicName);
    }

}
