package com.ywj.dao;

import com.ywj.entity.po.User;
import org.apache.ibatis.annotations.Mapper;

/**
 * @program: datacenter
 * @description: 测试实体dao
 * @author: yang
 * @create: 2020-10-17 09:25
 */
@Mapper
public interface UserMapper {

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
    int insert(User record);

    /**
     * 增量插入
     * @param record
     * @return
     */
    int insertSelective(User record);

    /**
     * 根据id获取数据
     * @param id
     * @return
     */
    User selectByPrimaryKey(Integer id);

    /**
     * 增量更新
     * @param record
     * @return
     */
    int updateByPrimaryKeySelective(User record);

    /**
     * 更新
     * @param record
     * @return
     */
    int updateByPrimaryKey(User record);
}