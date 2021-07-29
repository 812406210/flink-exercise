package com.canal.app.mapper;


import com.canal.app.entity.HiveChat;
import org.apache.ibatis.annotations.Mapper;


/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-03-18 14:00
 */
@Mapper
public interface HiveChatMapper {

    HiveChat selectById(Integer key);

    void insertChat(HiveChat chat);

    void updateChat(HiveChat chat);

}
