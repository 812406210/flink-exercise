package com.ywj.service;

import com.alibaba.fastjson.JSONObject;

import java.util.List;
import java.util.Map;

/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-08-13 13:53
 */
public interface AdverisePageService {

   List<JSONObject> getGraphData(JSONObject jsonObject);

}
