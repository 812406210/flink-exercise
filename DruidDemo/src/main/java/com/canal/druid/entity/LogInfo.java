package com.canal.druid.entity;

import lombok.Data;

/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-03-25 15:28
 */
@Data
public class LogInfo {

    private String __time;
    private String latencyMs;
    private String url;
    private String user;

}
