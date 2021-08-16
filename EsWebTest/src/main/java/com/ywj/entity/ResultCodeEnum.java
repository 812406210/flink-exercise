package com.ywj.entity;

/**
 * @program: datacenter
 * @description: 结果枚举
 * @author: yang
 * @create: 2020-10-17 09:25
 */
public enum ResultCodeEnum {
    SUCCESS(true,200,"成功"),
    UNKNOWN_ERROR(false,401,"未知参数"),
    PARAM_ERROR(false,402,"参数错误"),
    UPLOAD_FILE_ERROR(false,10000,"上传文件失败！"),
    DOWNLOAD_FILE_ERROR(false,10001,"下载文件失败！")

    ;

    private Boolean success;
    private Integer code;
    private String message;

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    ResultCodeEnum(Boolean success, Integer code, String message) {
        this.success = success;
        this.code = code;
        this.message = message;
    }
}