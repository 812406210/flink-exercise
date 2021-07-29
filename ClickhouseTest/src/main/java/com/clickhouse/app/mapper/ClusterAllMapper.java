package com.clickhouse.app.mapper;


import com.clickhouse.app.entity.ClusterAll;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @program: demo
 * @description:
 * @author: yang
 * @create: 2021-01-26 19:18
 */
@Mapper
public interface ClusterAllMapper {
    // 写入数据
    void saveData (ClusterAll clusterAll) ;
    // ID 查询
    ClusterAll selectById (@Param("id") Integer id) ;
    // 查询全部
    List<ClusterAll> selectList () ;
}