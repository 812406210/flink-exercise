package com.ywj.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.neo4j.ogm.annotation.GeneratedValue;
import org.neo4j.ogm.annotation.Id;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Property;

/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-07-08 14:40
 */
@NodeEntity(label = "dept")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Dept {

//    public Dept() {
//    }
//
//    public Dept(Long id, String deptName) {
//        this.id = id;
//        this.deptName = deptName;
//    }

    @Id
    @GeneratedValue
    private Long id;

    @Property(name = "deptName")
    private String deptName;

}
