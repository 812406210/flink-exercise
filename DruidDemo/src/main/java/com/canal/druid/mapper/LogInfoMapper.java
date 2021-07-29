package com.canal.druid.mapper;

import com.canal.druid.entity.GroupByInfo;
import com.canal.druid.entity.LogInfo;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-03-25 18:07
 */
@Mapper
public interface LogInfoMapper {

//    @Select("select __time,latencyMs,user,url from topic_log_db where latencyMs = #{latencyMs}")
    List<LogInfo> selectByLatencyMs(@Param("latencyMs") String latencyMs);

    @Select("SELECT __time,latencyMs,user,url FROM topic_log_db")
    List<LogInfo> selectAll();

    List<GroupByInfo> groupByLatencyMs();


}
