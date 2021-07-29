import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-06-23 10:36
 */
public class TestDate {
    public static void main(String[] args) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        System.out.println(sdf.format(getSundayWeek(new Date())));
    }

    public static Date getSundayWeek(Date date) {

        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        if(Calendar.SUNDAY != cal.get(Calendar.DAY_OF_WEEK)){
            cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY); // 获取本周日的日期
            cal.add(Calendar.WEEK_OF_YEAR, 1);
            date = cal.getTime();
        }
        return date;
    }
}
