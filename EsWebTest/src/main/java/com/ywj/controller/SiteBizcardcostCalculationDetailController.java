//package com.ywj.controller;
//
//import com.ywj.entity.ResponseResult;
//import com.ywj.entity.Result;
//import com.ywj.entity.vo.SiteBizCalculateDetailVO;
//import com.ywj.service.SiteBizcardcostService;
//import io.swagger.annotations.Api;
//import io.swagger.annotations.ApiImplicitParam;
//import io.swagger.annotations.ApiImplicitParams;
//import io.swagger.annotations.ApiOperation;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.web.bind.annotation.*;
//import javax.servlet.http.HttpServletRequest;
//import javax.servlet.http.HttpServletResponse;
//import java.util.List;
//
///**
// * @program: datacenter
// * @description: 军团均摊详细信息
// * @author: yang
// * @create: 2020-10-17 09:25
// */
//@RestController
//@RequestMapping("/site/biz")
//@Api(tags = "站点服务接口", description = "提供网络的 Rest API")
//public class SiteBizcardcostCalculationDetailController {
//
//    @Autowired
//    private SiteBizcardcostService siteBizcardcostService;
//
//
//    @RequestMapping("/getDetail")
//    @ResponseBody
//    @ApiOperation(nickname = "getDetail",value="军团的站点均摊信息(默认时间为null,既查询所有)",httpMethod = "GET")
//    @ApiImplicitParams({
//            @ApiImplicitParam(name="index",value="es库",required=true,defaultValue = "site_bizcardcost_calculation_detail" ),
//            @ApiImplicitParam(name="type",value="es表",required=true,defaultValue = "site_bizcardcost_calculation_detail"),
//            @ApiImplicitParam(name="accountNames",value="账户名(可修改)",required=true,defaultValue = "zhengqin01,chenlinwei"),
//            @ApiImplicitParam(name="startTime",value="开始时间(可修改),格式:2020-08-22",required=true,defaultValue = "null"),
//            @ApiImplicitParam(name="endTime",value="结束时间(可修改),格式:2020-08-22",required=true,defaultValue = "null")
//            })
//    public Result getDetail(@RequestParam(value = "index" ) String index, @RequestParam(value = "type" ) String type, @RequestParam(value = "accountNames" ) String accountNames,
//                            @RequestParam(value = "startTime" ) String startTime, @RequestParam(value = "endTime") String endTime) {
//
//        List<SiteBizCalculateDetailVO> detail = this.siteBizcardcostService.getDetail(index, type, accountNames, startTime, endTime);
//        return ResponseResult.success(detail);
//    }
//
//    @RequestMapping(value = "/getDetail2Excel", method = RequestMethod.GET)
//    @ResponseBody
//    @ApiOperation(nickname = "getDetail2Excel",value="军团的站点均摊信息保存至excel下载",httpMethod = "GET")
//    @ApiImplicitParams({
//            @ApiImplicitParam(name="index",value="es库",required=true,defaultValue = "site_bizcardcost_calculation_detail" ),
//            @ApiImplicitParam(name="type",value="es表",required=true,defaultValue = "site_bizcardcost_calculation_detail"),
//            @ApiImplicitParam(name="accountNames",value="账户名(可修改)",required=true,defaultValue = "zhengqin01"),
//            @ApiImplicitParam(name="startTime",value="开始时间(可修改),格式:2020-08-22",required=true,defaultValue = "null"),
//            @ApiImplicitParam(name="endTime",value="开始时间(可修改),格式:2020-08-22",required=true,defaultValue = "null")
//    })
//    public void getDetail2Excel(HttpServletResponse response, HttpServletRequest request,
//                                @RequestParam(value = "index" ) String index, @RequestParam(value = "type" ) String type, @RequestParam(value = "accountNames" ) String accountNames,
//                                @RequestParam(value = "startTime" ) String startTime, @RequestParam(value = "endTime") String endTime
//    ) throws Exception {
//        this.siteBizcardcostService.getDetail2Excel(response, request, index, type, accountNames, startTime, endTime);
//    }
//
//
//
//}
