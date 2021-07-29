package com.clickhouse.app.controller;


import com.clickhouse.app.entity.ClusterAll;
import com.clickhouse.app.mapper.ClusterAllMapper;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;

/**
 * @program: demo
 * @description:
 * @author: yang
 * @create: 2021-01-26 18:57
 */
@RestController
@RequestMapping("/ck")
public class CKController {

    @Resource
    private ClusterAllMapper clusterAllMapper;

    @RequestMapping("/test")
    public String test () {
        return "test";
    }

    @RequestMapping("/saveData")
    public String saveData (@RequestParam Integer id){
        ClusterAll clusterAll = new ClusterAll () ;
        clusterAll.setId(id);
        clusterAll.setWebsite("http://www.baidu.com");
        clusterAll.setWechat("yyyy");
        clusterAll.setFlightDate("2021-1-27");
        clusterAll.setYear("2020");
        clusterAllMapper.saveData(clusterAll);
        return "sus";
    }
    @RequestMapping("/selectById")
    public ClusterAll selectById (@RequestParam Integer id) {
        return clusterAllMapper.selectById(id) ;
    }
    @RequestMapping("/selectList")
    public List<ClusterAll> selectList () {
        return clusterAllMapper.selectList() ;
    }
}
