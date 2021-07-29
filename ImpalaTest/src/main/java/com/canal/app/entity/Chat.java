package com.canal.app.entity;

import lombok.Data;

/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-03-18 13:57
 */
@Data
public class Chat {
    private Integer key;
    private String chatName;
    private String createTime;
    private String remark;
}
