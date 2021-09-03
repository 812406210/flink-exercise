package com.ywj.serivce;

import com.ywj.entity.RelationShip;
import org.springframework.data.neo4j.repository.Neo4jRepository;
import org.springframework.stereotype.Repository;
/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create:
 */
@Repository
public interface RelationShipRepository extends Neo4jRepository<RelationShip, Long> {

}

