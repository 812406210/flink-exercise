import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-08-16 11:18
 */
public class Test {

    @org.junit.Test
    public  void test1(){
        List<Map> list= new ArrayList<>();
        JSONObject js = new JSONObject();
        js.put("num",1);
        for (int i = 0; i < 2; i++) {
//            Map map = new HashMap();
//            map.put(i,i);
            JSONObject js1 = new JSONObject();
            js1 = js;
            JSONObject js2 = new JSONObject();
            js2 = js1;
            js2.put(i+"",i);
            list.add(js2);
        }
        System.out.println(list);
    }
}
