package com.ywj.serivce;

import com.ywj.entity.RelationShip;
import org.springframework.data.neo4j.repository.Neo4jRepository;
import org.springframework.stereotype.Repository;
/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-07-08 14:42
 */
@Repository
public interface RelationShipRepository extends Neo4jRepository<RelationShip, Long> {

}

