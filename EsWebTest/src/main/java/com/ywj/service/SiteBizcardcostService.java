package com.ywj.service;

import com.ywj.entity.vo.SiteBizCalculateDetailVO;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;

/**
 * @program: datacenter
 * @description: 站点业务名片计算
 * @author: yang
 * @create: 2020-10-18 14:47
 */
public interface SiteBizcardcostService {

    /**
     * 站点业务名片计算详细信息
     * @param index
     * @param type
     * @param accountNames
     * @param startTime
     * @param endTime
     * @return
     */
    List<SiteBizCalculateDetailVO> getDetail(String index, String type, String accountNames, String startTime, String endTime);

    /**
     * 站点业务名片计算详细信息 保存至excel中，并下载
     * @param response
     * @param request
     * @param index
     * @param type
     * @param accountNames
     * @param startTime
     * @param endTime
     * @throws Exception
     */
    void getDetail2Excel(HttpServletResponse response, HttpServletRequest request, String index,  String type, String accountNames, String startTime, String endTime) throws Exception;
}
