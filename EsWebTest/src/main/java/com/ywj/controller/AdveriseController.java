package com.ywj.controller;

import com.alibaba.fastjson.JSONObject;
import com.ywj.config.EsClientConfig;
import com.ywj.entity.ResponseResult;
import com.ywj.entity.Result;
import com.ywj.service.AdverisePageService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.elasticsearch.action.get.GetResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-08-13 13:50
 */
@RestController
@RequestMapping("/page")
@Api(tags = "落地页统计", description = "落地页统计 Rest API")
public class AdveriseController {


    @Autowired
    private AdverisePageService adverisePageService;

    @Autowired
    private EsClientConfig esClient;

    @RequestMapping("/getDataByIdES6")
    @ResponseBody
    @ApiOperation(nickname = "getDataByIdES6",value="ES6.6版本测试",httpMethod = "POST")
    public Result getDataByIdES6( @RequestBody JSONObject data) {
        List<JSONObject> graphData = adverisePageService.getGraphData(data);
        return ResponseResult.success(graphData);
    }


    @RequestMapping("/esJson")
    @ResponseBody
    @ApiOperation(nickname = "esJson",value="esJson",httpMethod = "POST")
    public Result esJson(  @RequestParam(value = "requestJson" ,required = true ,defaultValue = "") String requestJson
    ) {

        return ResponseResult.success();
    }

}
