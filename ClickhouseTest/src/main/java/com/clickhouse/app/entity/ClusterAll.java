package com.clickhouse.app.entity;

/**
 * @program: demo
 * @description:
 * @author: yang
 * @create: 2021-01-27 10:09
 */
public class ClusterAll {

    private Integer id ;
    private String website;
    private String wechat;
    private String FlightDate;
    private String Year;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getWebsite() {
        return website;
    }

    public void setWebsite(String website) {
        this.website = website;
    }

    public String getWechat() {
        return wechat;
    }

    public void setWechat(String wechat) {
        this.wechat = wechat;
    }

    public String getFlightDate() {
        return FlightDate;
    }

    public void setFlightDate(String flightDate) {
        FlightDate = flightDate;
    }

    public String getYear() {
        return Year;
    }

    public void setYear(String year) {
        Year = year;
    }
}
