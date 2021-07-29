package com.canal.app.service;

import com.canal.app.entity.HiveChat;
import com.canal.app.mapper.HiveChatMapper;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-03-18 14:04
 */
@Component
public class HiveChatService {

    @Resource
    private HiveChatMapper hiveChatMapper;

    public HiveChat selectById(Integer id ) {
        HiveChat chat = new HiveChat();
        try {
            long start = System.currentTimeMillis();
            chat  =  hiveChatMapper.selectById(id);
            long end = System.currentTimeMillis() ;
            System.out.println("共花时间"+(end-start)/1000+"秒");
           // System.out.println("查询结果为:"+chat);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return chat;
    }



    public void updateChat(HiveChat chat) {
        try {
            hiveChatMapper.updateChat(chat);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void insertChat(HiveChat chat) {
        try {
            hiveChatMapper.insertChat(chat);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
