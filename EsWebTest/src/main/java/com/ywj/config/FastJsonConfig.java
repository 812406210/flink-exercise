package com.ywj.config;

import com.alibaba.fastjson.parser.ParserConfig;
import org.springframework.stereotype.Component;

/**
 * @program: datacenter
 * @description: fastjson漏洞修复, 参考官网:https://github.com/alibaba/fastjson/wiki/fastjson_safemode
 * @author: yang
 * @create: 2020-10-18 14:45
 */
@Component
public class FastJsonConfig {
    static {
        ParserConfig.getGlobalInstance().setSafeMode(true);
    }
}
