package com.canal.phonix.entity;

import com.canal.phonix.hbaseUtils.HbaseColumn;
import com.canal.phonix.hbaseUtils.HbaseTable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: yangwj
 * @Descriptions:
 * @Date: Created in 2018/3/22
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
@HbaseTable(tableName = "demo")
public class Demo {

    @HbaseColumn(family = "rowkey", qualifier = "rowkey")
    private String id;

    @HbaseColumn(family = "info", qualifier = "content")
    private String content;

    @HbaseColumn(family = "info", qualifier = "avg")
    private String avg;

}
