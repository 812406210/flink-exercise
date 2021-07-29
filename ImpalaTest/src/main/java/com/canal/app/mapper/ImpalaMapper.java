package com.canal.app.mapper;

import com.canal.app.entity.Chat;
import org.apache.ibatis.annotations.Mapper;


/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-03-18 14:00
 */
@Mapper
public interface ImpalaMapper {

    Chat selectById(Integer key);

    void insertChat(Chat chat);

    void updateChat(Chat chat);

}
