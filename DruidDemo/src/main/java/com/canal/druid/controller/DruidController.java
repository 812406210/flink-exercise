package com.canal.druid.controller;//package com.yangwj.druid.controller;

import com.canal.druid.mapper.LogInfoMapper;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;
import java.util.List;
import com.canal.druid.entity.*;

import javax.annotation.Resource;


/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-03-24 18:00
 */

@RestController
@RequestMapping("/druid")
public class DruidController {

    @Resource
    private LogInfoMapper logInfoMapper;

    @GetMapping("/selectbyLatencyMs")
    public String selectbyLatencyMs(@RequestParam("latencyMs") String latencyMs) throws SQLException {
        List<LogInfo> logInfos = logInfoMapper.selectByLatencyMs(latencyMs);
        System.out.println(logInfos);
        return "新增记录成功！";
    }

    @GetMapping("/selectAll")
    public String selectAll() {
        List<LogInfo> logInfos = logInfoMapper.selectAll();
        System.out.println(logInfos);
        return "新增记录成功！";
    }


    @GetMapping("/groupByLatencyMs")
    public String groupByLatencyMs() {
        List<GroupByInfo> groupByInfos = logInfoMapper.groupByLatencyMs();
        System.out.println(groupByInfos);
        return "分组";
    }
}
