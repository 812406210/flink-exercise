package com.ywj.dao;


import com.ywj.entity.po.Weblog;

/**
 * @program: datacenter
 * @description: 日志实体dao
 * @author: yang
 * @create: 2020-10-17 09:25
 */
public interface WeblogMapper {
    /**
     * 删除
     * @param id
     * @return
     */
    int deleteByPrimaryKey(Integer id);

    /**
     * 插入
     * @param record
     * @return
     */
    int insert(Weblog record);

    /**
     * 增量插入
     * @param record
     * @return
     */
    int insertSelective(Weblog record);

    /**
     * 根据id获取数据
     * @param id
     * @return
     */
    Weblog selectByPrimaryKey(Integer id);

    /**
     * 增量修改
     * @param record
     * @return
     */
    int updateByPrimaryKeySelective(Weblog record);

    /**
     * 修改
     * @param record
     * @return
     */
    int updateByPrimaryKey(Weblog record);
}