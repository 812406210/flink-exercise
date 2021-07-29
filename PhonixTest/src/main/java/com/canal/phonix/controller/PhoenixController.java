package com.canal.phonix.controller;


import com.canal.phonix.phonixUtils.PhoenixUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;

/**
 * @program: hushuo-cdh
 * @description: 控制器
 * @author: yang
 * @create: 2020-11-03 14:40
 */
@RestController
public class PhoenixController {
    @Autowired
    private PhoenixUtil phoenixUtil;

    @RequestMapping("/getOne")
    public String getOne() throws SQLException {
        String sql = "select * from TEST where mykey =2";
        phoenixUtil.readByKey(sql);
        return "success";
    }

}
