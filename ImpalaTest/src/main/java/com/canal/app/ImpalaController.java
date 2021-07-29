package com.canal.app;

import com.alibaba.fastjson.JSON;
import com.canal.app.entity.Chat;
import com.canal.app.service.ChatService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-03-18 13:22
 */
@RestController
@RequestMapping("/test")
public class ImpalaController {

    @Resource
    private ChatService impalaService;

    @GetMapping("/insert")
    public String insertChat() {
        Chat chat = new Chat();
        chat.setKey(1);
        chat.setChatName("张三");
        impalaService.insertChat(chat);
        return "新增记录成功！";
    }

    @Deprecated
    @GetMapping("/update")
    public String updateChat() {
        Chat chat = new Chat();
        chat.setKey(1);
        chat.setChatName("张三");

        //todo impala 不支持修改 非kudu表
        impalaService.updateChat(chat);
        return "新增记录成功！";
    }

    @GetMapping("/select")
    public String selectChat(@RequestParam Integer id) {

        return  JSON.toJSONString(impalaService.selectById(id));

    }

}
