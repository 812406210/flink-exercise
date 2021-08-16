package com.ywj.entity;

/**
 * @program: datacenter
 * @description: 所有返回实体类
 * @author: yang
 * @create: 2020-10-17 09:25
 */
public class ResponseResult {

    /**成功且带数据**/
    public static Result success(Object object){
        Result result = new Result();
        result.setCode(ResultCodeEnum.SUCCESS.getCode());
        result.setMsg(ResultCodeEnum.SUCCESS.getMessage());
        result.setData(object);
        return result;
    }

    /**成功但不带数据**/
    public static Result success(){

        return success(null);
    }
    /**失败**/
    public static Result error(Integer code,String msg){
        Result result = new Result();
        result.setCode(code);
        result.setMsg(msg);
        return result;
    }
}
