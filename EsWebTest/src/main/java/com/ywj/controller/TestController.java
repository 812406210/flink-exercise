package com.ywj.controller;

import com.alibaba.fastjson.JSONObject;
import com.ywj.config.EsClientConfig;
import com.ywj.dao.UserMapper;
import com.ywj.entity.ResponseResult;
import com.ywj.entity.Result;
import com.ywj.entity.po.User;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.elasticsearch.action.get.GetResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.Map;


/**
 * @program: datacenter
 * @description: 网络测试类
 * @author: yang
 * @create: 2020-10-17 09:25
 */
@RestController
@RequestMapping("/test")
@Api(tags = "项目网络测试相关接口", description = "提供网络测试的 Rest API")
public class TestController {

    @Autowired
    private EsClientConfig esClient;

    @Autowired
    private UserMapper userMapper;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @RequestMapping("/run")
    @ResponseBody
    @ApiOperation(nickname = "run",value="应用运行测试",httpMethod = "GET")
    public Result run() {
        return ResponseResult.success("Application work success");
    }

    //ES6.6版本测试
    @RequestMapping("/getDataByIdES6")
    @ResponseBody
    @ApiOperation(nickname = "getDataByIdES6",value="ES6.6版本测试",httpMethod = "GET")
    public  Result getDataByIdES6(
            @RequestParam(value = "index" ,required = true ,defaultValue = "goods") String index,
            @RequestParam(value = "type" ,required = true ,defaultValue = "_doc") String type,
            @RequestParam(value = "id" ,required = true ,defaultValue = "2") String id
    ) {
        GetResponse response = esClient.initEsClient().prepareGet(index, type, id).execute().actionGet();
        Map<String, Object> result = response.getSource();
        System.out.println("12");
        return ResponseResult.success(result);
    }

    @RequestMapping("/getUser")
    @ResponseBody
    @ApiOperation(nickname = "getUser",value="测试mybatis",httpMethod = "GET")
    public  Result getUser(
            @RequestParam(value = "id" ,required = true ,defaultValue = "1") String id
    ) {
        User user = this.userMapper.selectByPrimaryKey(Integer.parseInt(id));
        return ResponseResult.success(user);
    }

    @PostMapping("/getUserPost")
    @ResponseBody
    @ApiOperation(nickname = "getUserPost",value="测试mybatis",httpMethod = "POST")
    public  Result getUserPost(
            @RequestBody String jsonStr) {
        System.out.println(jsonStr);
        String[] split = jsonStr.split("&");
        JSONObject jsonObject = new JSONObject();
        Arrays.stream(split).forEach(s -> {
            String[] keyValue = s.split("=");
            jsonObject.put(keyValue[0],keyValue[1]);
        });
        System.out.println(jsonObject);
        User user = this.userMapper.selectByPrimaryKey(Integer.parseInt(jsonObject.getString("id")));
        return ResponseResult.success(user);
    }

    @RequestMapping("/testRedis")
    @ResponseBody
    @ApiOperation(nickname = "testRedis",value="Redis测试",httpMethod = "GET")
    public  Result testRedis() {
        redisTemplate.opsForValue().set("hello","world");
        return ResponseResult.success();
    }

    @RequestMapping("/getFromRedis")
    @ResponseBody
    @ApiOperation(nickname = "getFromRedis",value="Redis拿数",httpMethod = "GET")
    public  Result getFromRedis() {
        String helloValue = redisTemplate.opsForValue().get("hello");
        return ResponseResult.success(helloValue);
    }
}
