package com.ywj.serivce;

import com.ywj.entity.Dept;
import org.springframework.data.neo4j.repository.Neo4jRepository;
import org.springframework.stereotype.Repository;
/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-07-08 14:42
 */
@Repository
public interface DeptRepository extends Neo4jRepository<Dept,Long> {

}

