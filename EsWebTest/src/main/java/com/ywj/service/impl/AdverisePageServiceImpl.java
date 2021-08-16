package com.ywj.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mysql.cj.xdevapi.JsonArray;
import com.ywj.config.EsClientConfig;
import com.ywj.service.AdverisePageService;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.NumberFormat;
import java.util.*;

/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-08-13 13:53
 */
@Service
public class AdverisePageServiceImpl implements AdverisePageService {


    @Autowired
    private EsClientConfig esClientConfig;

    private  final String DH_CALLRECORD = "dh_callrecord_v1";

    public final static Map dimensionMap = new HashMap();
    static {
        dimensionMap.put("day", "by_day");
        dimensionMap.put("week", "by_week");
        dimensionMap.put("month", "by_month");
        dimensionMap.put("tenant_id", "by_tenant_id");
        dimensionMap.put("caller", "by_caller");
        dimensionMap.put("province", "by_province");
    }

    @Override
    public List<JSONObject> getGraphData(JSONObject requestJson) {


        //参数校验
        //字段维度
        String dimension = requestJson.getString("dimension") != null ? requestJson.getString("dimension") : null;
        //时间维度
        String timeDimension = requestJson.getString("timeDimension");

        DateHistogramInterval dateHistogramInterval =   "day".equals(timeDimension) ? DateHistogramInterval.DAY :
                "week".equals(timeDimension) ? DateHistogramInterval.WEEK :
                        "month".equals(timeDimension) ? DateHistogramInterval.MONTH: DateHistogramInterval.DAY;

        String index = dimension !=null ? dimension : timeDimension;
        if(Objects.isNull(dimension) && Objects.isNull(timeDimension)) {return  null;}

        TransportClient client = esClientConfig.initEsClient();
        //组件条件
        SearchRequest searchRequest = new SearchRequest(DH_CALLRECORD).types("_doc");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.aggregation(this.filterAggConditions(requestJson,dateHistogramInterval,dimension));
        System.out.println(builder.toString());
        searchRequest.source(builder);
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest).get();
        }catch (Exception e){
            e.printStackTrace();
        }


        //获取数据
        Aggregations aggregations = searchResponse.getAggregations();
        String[] dimensions = dimension.split(",");
        Histogram aggregation = aggregations.get(dimensionMap.get(timeDimension).toString());

        List<JSONObject>  listjson = new ArrayList<JSONObject>();

        for (Histogram.Bucket bucket:aggregation.getBuckets()) {
            String timeKey = bucket.getKeyAsString();
            long timeCount = bucket.getDocCount();

            JSONObject timeJson = new JSONObject();
            timeJson.put("timeKey",timeKey);
            timeJson.put("timeCount",timeCount);

            Aggregations tenantAgg = bucket.getAggregations();
            Terms groupByTenant = tenantAgg.get((String) dimensionMap.get(dimensions[0]));

            for (Terms.Bucket tenantBucket:groupByTenant.getBuckets()) {
                JSONObject tenantJson = new JSONObject();
                tenantJson = timeJson;
                String tenantKey = tenantBucket.getKeyAsString();
                long tenantBucketDocCount = tenantBucket.getDocCount();

                Aggregations callerAgg = tenantBucket.getAggregations();
                Terms groupbyCaller = callerAgg.get((String) dimensionMap.get(dimensions[1]));

                tenantJson.put("tenant_id",tenantKey);
                tenantJson.put("tenantBucketDocCount",tenantBucketDocCount);

                for (Terms.Bucket callBucket :groupbyCaller.getBuckets()) {
                    String callKey = callBucket.getKeyAsString();
                    long callBucketDocCount = callBucket.getDocCount();
                    JSONObject callJson = new JSONObject();
                    callJson.put("tenant_id",tenantJson.getString("tenant_id"));
                    callJson.put("tenantBucketDocCount",tenantJson.getLong("tenantBucketDocCount"));
                    callJson.put("timeKey",tenantJson.getString("timeKey"));
                    callJson.put("timeCount",tenantJson.getLong("timeCount"));
                    callJson.put("call_id",callKey);
                    callJson.put("callBucketDocCount",callBucketDocCount);
                    listjson.add(callJson);
                }
            }

        }
        System.out.println("jsonList--------------"+listjson);
//        List<JSONObject> rateList = calculateValueList(jsonList);
//        System.out.println("rateList======="+rateList);
        //计算百分比
        return listjson;
    }


    private List<JSONObject> calculateValueList(List<JSONObject> list) {
        List<JSONObject> rateList = new ArrayList<>();
        for(int i =0;i< list.size();i++){
            JSONObject jsonObject = new JSONObject();
            Double idSum = (Double)list.get(i).get("idSum");
            Double ageSum = (Double)list.get(i).get("ageSum");

//            Double idSum1 = (Double)list.get(i+1).get("idSum");
//            Double ageSum1 = (Double)list.get(i+1).get("ageSum");
            String idRate = getRate(idSum, ageSum);
            //String ageRate = getRate(ageSum, ageSum1);
            jsonObject.putIfAbsent("time",list.get(i).get("timeKey"));
            jsonObject.putIfAbsent("idRate",idRate);
            //jsonObject.putIfAbsent("ageRate",ageRate);
            rateList.add(jsonObject);
        }
        return rateList;
    }


    public static String getRate(Double value,Double totalSum){
        NumberFormat numberFormat = NumberFormat.getInstance();
        numberFormat.setMaximumFractionDigits(2);
        String result = numberFormat.format((Double)value/(Double)totalSum*100);
        return result;
    }

    /**聚合条件
     * @return*/
    private DateHistogramAggregationBuilder filterAggConditions(JSONObject requestJson, DateHistogramInterval interval, String dimension){
        DateHistogramAggregationBuilder histogramAggregationBuilder = null;
        String[] dimensions = dimension.split(",");
        //时间维度
        if(Objects.nonNull(requestJson.getString("timeDimension"))){
            //eg：day
            histogramAggregationBuilder = AggregationBuilders.dateHistogram((String) dimensionMap.get(requestJson.getString("timeDimension")))
                    .dateHistogramInterval(interval).field("create_ts");

            TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms((String) dimensionMap.get(dimensions[0])).field(dimensions[0]);
            TermsAggregationBuilder termsAggregationBuilder1 = AggregationBuilders.terms((String) dimensionMap.get(dimensions[1])).field(dimensions[1]);
            termsAggregationBuilder.subAggregation(termsAggregationBuilder1);
            histogramAggregationBuilder.subAggregation(termsAggregationBuilder);
        }
        System.out.println(histogramAggregationBuilder.toString());
        return  histogramAggregationBuilder;
    }


}
