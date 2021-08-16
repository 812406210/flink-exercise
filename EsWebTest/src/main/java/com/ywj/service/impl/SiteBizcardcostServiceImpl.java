package com.ywj.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.ywj.config.EsClientConfig;
import com.ywj.entity.vo.SiteBizCalculateDetailVO;
import com.ywj.service.SiteBizcardcostService;
import com.ywj.utils.ExcelData;
import com.ywj.utils.ExcelUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @program: datacenter
 * @description: 站点业务名片计算
 * @author: yang
 * @create: 2020-10-18 14:48
 */
@Service
public class SiteBizcardcostServiceImpl implements SiteBizcardcostService {

    private static final Logger log = LoggerFactory.getLogger(SiteBizcardcostServiceImpl.class);

    @Autowired
    private EsClientConfig esClient;


    @Override
    public List<SiteBizCalculateDetailVO> getDetail(String index, String type, String accountNames, String startTime, String endTime) {

        log.info("[request params]: index ={}, type= {}, ids= {}",index,type,accountNames);
        SiteBizCalculateDetailVO siteBizCalculateDetailVO = null;
        List<SiteBizCalculateDetailVO> retList = new ArrayList<SiteBizCalculateDetailVO>();
        String[] countArray = accountNames.split(",");
        TransportClient client = esClient.initEsClient();
        MultiSearchRequestBuilder multiSearchRequestBuilder = client.prepareMultiSearch();
        //多字段多条件查询
        for (String count:countArray) {
            SearchRequestBuilder requestBuilder = client.prepareSearch(index);
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            boolQuery.must(QueryBuilders.termQuery("cf.level_studio_resp_account.raw",count));
            if("null".equals(startTime) && "null".equals(endTime)){
                log.info("startTime and endTime is null");
            }else{
                boolQuery.must(QueryBuilders.rangeQuery("cf.biz_date").gte(startTime).lte(endTime));
            }
            requestBuilder.setQuery(boolQuery);
            requestBuilder.addSort(SortBuilders.fieldSort("cf.biz_date").order(SortOrder.DESC));
            requestBuilder.setSize(50);
            multiSearchRequestBuilder.add(requestBuilder);
        }
        //打印查询条件
        multiSearchRequestBuilder.request().requests().forEach(searchRequest ->log.info("查询条件: "+searchRequest.toString()));
        MultiSearchResponse items = multiSearchRequestBuilder.get();
        for (MultiSearchResponse.Item response:items) {
            SearchHit[] hits = response.getResponse().getHits().getHits();
            for (SearchHit hit:hits) {
                String source = hit.getSourceAsString();
                JSONObject jsonObject = JSONObject.parseObject(source);
                JSONObject cfData = jsonObject.getJSONObject("cf");
                siteBizCalculateDetailVO = JSON.parseObject(cfData.toJSONString(), SiteBizCalculateDetailVO.class);
                retList.add(siteBizCalculateDetailVO);
                log.info(siteBizCalculateDetailVO.toString());
            }
        }
        return retList;
    }

    @Override
    public void getDetail2Excel(HttpServletResponse response, HttpServletRequest request, String index, String type, String accountNames, String startTime, String endTime) throws Exception{
        ExcelData data = new ExcelData();
        data.setName("军团的站点均摊信息保存至excel下载");
        List<String> titles = new ArrayList();
        Class<?> aClass = Class.forName("com.hushuo.datacenter.entity.vo.SiteBizCalculateDetailVO");
        Field[] declaredFields = aClass.getDeclaredFields();
        //标题
        // this.setExcelTitle(titles);
        for (Field field:declaredFields) {
            titles.add(field.getName());
        }
        data.setTitles(titles);

        //数据
        List<List<Object>> rows = new ArrayList();
        List<SiteBizCalculateDetailVO> detailData = this.getDetail(index, type, accountNames, startTime, endTime);

        detailData.stream().forEach(siteBizCalculateDetailVO -> {
            List<Object> row = new ArrayList();
            row.add(StringUtils.isBlank(siteBizCalculateDetailVO.getBiz_date())? "无":siteBizCalculateDetailVO.getBiz_date());
            row.add(StringUtils.isBlank(siteBizCalculateDetailVO.getStudio())? "无":siteBizCalculateDetailVO.getStudio());
            row.add(StringUtils.isBlank(siteBizCalculateDetailVO.getCash_amount())? "无":siteBizCalculateDetailVO.getCash_amount());
            row.add(StringUtils.isBlank(siteBizCalculateDetailVO.getOriginal_cash_ratio())? "无":siteBizCalculateDetailVO.getOriginal_cash_ratio());
            row.add(StringUtils.isBlank(siteBizCalculateDetailVO.getLevel_studio_cash_amount())?"无":siteBizCalculateDetailVO.getLevel_studio_cash_amount());
            row.add(StringUtils.isBlank(siteBizCalculateDetailVO.getLevel_studio_resp_account()) ? "无":siteBizCalculateDetailVO.getLevel_studio_resp_account());
            row.add(StringUtils.isBlank(siteBizCalculateDetailVO.getLevel_noplan_studio()) ? "无":siteBizCalculateDetailVO.getLevel_noplan_studio());
            row.add(StringUtils.isBlank(siteBizCalculateDetailVO.getCash_ratio()) ?"无":siteBizCalculateDetailVO.getCash_ratio());
            row.add(StringUtils.isBlank(siteBizCalculateDetailVO.getLevel_noplan_studio_cash_amount())?"无":siteBizCalculateDetailVO.getLevel_noplan_studio_cash_amount());
            row.add(StringUtils.isBlank(siteBizCalculateDetailVO.getOriginal_cash_amount())?"无":siteBizCalculateDetailVO.getOriginal_cash_amount());
            row.add(StringUtils.isBlank(siteBizCalculateDetailVO.getLevel_resp_account_cash_amount()) ? "无":siteBizCalculateDetailVO.getLevel_resp_account_cash_amount());
            row.add(StringUtils.isBlank(siteBizCalculateDetailVO.getLevel_four_cash_amount()) ?"无":siteBizCalculateDetailVO.getLevel_four_cash_amount());
            row.add(StringUtils.isBlank(siteBizCalculateDetailVO.getLevel_studio_account_cash_ratio())?"无":siteBizCalculateDetailVO.getLevel_studio_account_cash_ratio());
            row.add(StringUtils.isBlank(siteBizCalculateDetailVO.getLevel_noplan_studio_cash_ratio())?"无":siteBizCalculateDetailVO.getLevel_noplan_studio_cash_ratio());
            row.add(StringUtils.isBlank(siteBizCalculateDetailVO.getLevel_four_cash_ratio())?"无":siteBizCalculateDetailVO.getLevel_four_cash_ratio());
            row.add(StringUtils.isBlank(siteBizCalculateDetailVO.getLevel_resp_account_cash_ratio())?"无":siteBizCalculateDetailVO.getLevel_resp_account_cash_ratio());
            row.add(StringUtils.isBlank(siteBizCalculateDetailVO.getLevel_noplan_account_cash_ratio())?"无":siteBizCalculateDetailVO.getLevel_noplan_account_cash_ratio());
            row.add(StringUtils.isBlank(siteBizCalculateDetailVO.getSite_id())?"无":siteBizCalculateDetailVO.getSite_id());
            row.add(StringUtils.isBlank(siteBizCalculateDetailVO.getLevel_noplan_studio_account())?"无":siteBizCalculateDetailVO.getLevel_noplan_studio_account());
            row.add(StringUtils.isBlank(siteBizCalculateDetailVO.getResp_account())?"无":siteBizCalculateDetailVO.getResp_account());
            row.add(StringUtils.isBlank(siteBizCalculateDetailVO.getLevel_studio_cash_ratio())?"无":siteBizCalculateDetailVO.getLevel_studio_cash_ratio());
            row.add(StringUtils.isBlank(siteBizCalculateDetailVO.getLevel_noplan_account_cash_amount())?"无":siteBizCalculateDetailVO.getLevel_noplan_account_cash_amount());
            row.add(StringUtils.isBlank(siteBizCalculateDetailVO.getAccount())?"无":siteBizCalculateDetailVO.getAccount());
            //添加一行数据
            rows.add(row);
        });
        data.setRows(rows);

        SimpleDateFormat fdate=new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        String fileName=accountNames+":"+fdate.format(new Date())+".xls";
        ExcelUtils.exportExcel(response,fileName,data);
    }

    public void setExcelTitle(List<String> titles){
        titles.add("biz_date");
        titles.add("studio");
        titles.add("cash_amount");
        titles.add("original_cash_ratio");
        titles.add("level_studio_cash_amount");
        titles.add("level_studio_resp_account");
        titles.add("level_noplan_studio");
        titles.add("cash_ratio");
        titles.add("level_noplan_studio_cash_amount");
        titles.add("original_cash_amount");
        titles.add("level_resp_account_cash_amount");
        titles.add("level_four_cash_amount");
        titles.add("level_studio_account_cash_ratio");
        titles.add("level_noplan_studio_cash_ratio");
        titles.add("level_four_cash_ratio");
        titles.add("level_resp_account_cash_ratio");
        titles.add("level_noplan_account_cash_ratio");
        titles.add("site_id");
        titles.add("level_noplan_studio_account");
        titles.add("resp_account");
        titles.add("level_studio_cash_ratio");
        titles.add("level_noplan_account_cash_amount");
        titles.add("account");
    }
}
