import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

/**
 * @program: Flink12Doris
 * @description:
 * @author: yang
 * @create: 2021-06-29 13:46
 */
public class DataSourceConfig {
    private static DataSource dataSource = null;

    static {
        getInstance();
    }

    public static DataSource getInstance() {
        if (Objects.nonNull(dataSource)) {
            return dataSource;
        }
        try {
            System.out.println("初始化mysql数据连接池");
            //从druid.properties中读取数据
            Properties properties = new Properties();
            InputStream in = ManyKafka2HbaseAdapter.class.getResourceAsStream("application.properties");
            properties.load(in);
            dataSource = DruidDataSourceFactory.createDataSource(properties);
            return dataSource;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
