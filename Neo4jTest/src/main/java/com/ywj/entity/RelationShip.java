package com.ywj.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import lombok.NoArgsConstructor;
import org.neo4j.ogm.annotation.*;


/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-07-08 14:41
 */
@RelationshipEntity(type = "relationShip")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RelationShip {

//    public RelationShip() {
//    }
//
//    public RelationShip(Long id, Dept parent, Dept child) {
//        this.id = id;
//        this.parent = parent;
//        this.child = child;
//    }

    @Id
    @GeneratedValue
    private Long id;

    @StartNode
    private Dept parent;

    @EndNode
    private Dept child;
}

