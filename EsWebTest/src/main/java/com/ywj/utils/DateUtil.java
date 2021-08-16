package com.ywj.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @program: datacenter
 * @description: 时间转换
 * @author: yang
 * @create: 2020-10-18 15:18
 */
public class DateUtil {

    /**转换到天*/
    public final static String DATE_FORMAT_DAY = "yyyy-MM-dd";
    /**转换到秒*/
    public final static String DATE_FORMAT_SECOND = "yyyy-MM-dd HH:mm:ss";
    /**流水号之类*/
    public static String NOCHAR_PATTERN = "yyyyMMddHHmmss";


    /**
     * 获取当前时间
     * @return返回字符串格式
     */
    public static String getStringDate(String pattern) {
        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat(pattern);
        String dateString = formatter.format(currentTime);
        return dateString;
    }

    /**
     * 字符串转换为日期对象
     * @param dateStr 时间字符串
     * @param pattern 时间格式
     * @return
     */
    public static Date dateStr2Date(String dateStr,String pattern) {
        Date date = null;
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        try {
            date = format.parse(dateStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }

    /**
     * 将日期转为 字符串
     * @param date 时间
     * @param format 时间格式
     * @return
     */
    public static String dateToStr(Date date, String format) {
        if (date == null) { return null; }
        return new SimpleDateFormat(format).format(date);
    }

    /**
     * 生成流水号
     * @param t 流水号位数
     * @return 流水号
     */
    public static String getSequenceNumber(int t) {
        Date d = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat(NOCHAR_PATTERN);
        String str = sdf.format(d);
        String haomiao = String.valueOf(System.nanoTime());
        str = str + haomiao.substring(haomiao.length() - 6, haomiao.length());
        return str.substring(str.length() - t, str.length());
    }

    public static void main(String[] args) {
        //字符串转为时间
        Date date = dateStr2Date("2020-10-18 15:36:41", DATE_FORMAT_SECOND);
        System.out.println(date);

        //当前时间
        String stringDate = getStringDate(DATE_FORMAT_SECOND);
        System.out.println(stringDate);

        //时间转为字符串
        String str = dateToStr(new Date(), DATE_FORMAT_DAY);
        System.out.println(str);

        //流水号
        String sequenceNumber = getSequenceNumber(7);
        System.out.println(sequenceNumber);


    }
}
