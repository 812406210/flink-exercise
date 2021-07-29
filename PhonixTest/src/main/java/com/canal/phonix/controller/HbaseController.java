package com.canal.phonix.controller;


import com.canal.phonix.entity.Demo;
import com.canal.phonix.hbaseUtils.HBaseDaoUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @Author: yangwj
 * @Descriptions:
 * @Date: Created in 2018/3/21
 */
@RestController
@RequestMapping("/demo")
public class HbaseController {

    @Autowired
    private HBaseDaoUtil hBaseDaoUtil;

    @GetMapping("/save")
    public Object save() {
        Demo demo = new Demo("1", "2323", "sajosj");
        boolean r = hBaseDaoUtil.save(demo);
        return r;
    }

    @GetMapping("/get")
    public Object getBean() {
        List<Demo> demo = hBaseDaoUtil.get(new Demo(), "1");
        System.out.println(demo);
        return demo;
    }

    @GetMapping("/create")
    public String create() {
        String[] staffs = new String[]{"Tom", "Bob", "Jane"};

        List staffsList = Arrays.asList(staffs);

        Set result = new HashSet(staffsList);
       hBaseDaoUtil.createTable("demo",result);
        return "success";
    }
}
