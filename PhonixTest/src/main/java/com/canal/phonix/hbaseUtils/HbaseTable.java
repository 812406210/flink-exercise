package com.canal.phonix.hbaseUtils;

import java.lang.annotation.*;


@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE })
@Inherited
public @interface HbaseTable {
    String tableName() default "";
}
