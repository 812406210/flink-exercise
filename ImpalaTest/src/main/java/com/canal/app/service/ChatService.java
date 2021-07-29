package com.canal.app.service;

import com.canal.app.entity.Chat;
import com.canal.app.mapper.ImpalaMapper;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-03-18 14:04
 */
@Component
public class ChatService {

    @Resource
    private ImpalaMapper impalaMapper;

    public Chat selectById(Integer id ) {
        Chat chat = new Chat();
        try {
            long start = System.currentTimeMillis();
            chat  =  impalaMapper.selectById(id);
            long end = System.currentTimeMillis() ;
            System.out.println("共花时间"+(end-start)/1000+"秒");
           // System.out.println("查询结果为:"+chat);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return chat;
    }



    public void updateChat(Chat chat) {
        try {
            impalaMapper.updateChat(chat);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void insertChat(Chat chat) {
        try {
            impalaMapper.insertChat(chat);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
