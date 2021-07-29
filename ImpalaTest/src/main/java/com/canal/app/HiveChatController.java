package com.canal.app;

import com.alibaba.fastjson.JSON;
import com.canal.app.entity.HiveChat;
import com.canal.app.service.HiveChatService;
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
@RequestMapping("/chat")
public class HiveChatController {

    @Resource
    private HiveChatService hiveChatService;

    @GetMapping("/insert")
    public String insertChat() {
        HiveChat chat = new HiveChat();
        chat.setId(1);
        chat.setChatName("张三");
        hiveChatService.insertChat(chat);
        return "新增记录成功！";
    }

    @Deprecated
    @GetMapping("/update")
    public String updateChat() {
        HiveChat chat = new HiveChat();
        chat.setId(1);
        chat.setChatName("张三");

        //todo impala 不支持修改 非kudu表
        hiveChatService.updateChat(chat);
        return "新增记录成功！";
    }

    @GetMapping("/select")
    public String selectChat(@RequestParam Integer id) {

        return  JSON.toJSONString(hiveChatService.selectById(id));

    }

}
