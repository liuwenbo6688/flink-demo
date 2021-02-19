package com.datax.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by li on 2019/1/5.
 */
public class DateUtils {


    /**
     * 根据用户的年龄，拿到所属的年代标签
     *
     * @param age
     * @return
     */
    public static String getYearBaseByAge(String age) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.YEAR, -Integer.valueOf(age));
        Date date = calendar.getTime();
        DateFormat dateFormat = new SimpleDateFormat("yyyy");
        String dateStr = dateFormat.format(date);
        Integer dateInt = Integer.valueOf(dateStr);

        String yearBaseType = "未知";
        if (dateInt >= 1940 && dateInt < 1950) {
            yearBaseType = "40后";
        } else if (dateInt >= 1950 && dateInt < 1960) {
            yearBaseType = "50后";
        } else if (dateInt >= 1960 && dateInt < 1970) {
            yearBaseType = "60后";
        } else if (dateInt >= 1970 && dateInt < 1980) {
            yearBaseType = "70后";
        } else if (dateInt >= 1980 && dateInt < 1990) {
            yearBaseType = "80后";
        } else if (dateInt >= 1990 && dateInt < 2000) {
            yearBaseType = "90后";
        } else if (dateInt >= 2000 && dateInt < 2010) {
            yearBaseType = "00后";
        } else if (dateInt >= 2010) {
            yearBaseType = "10后";
        }

        return yearBaseType;
    }


    public static int getIntervalDaysBetween(String startTime, String endTime, String dateFormatStr) throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat(dateFormatStr);
        Date start = dateFormat.parse(startTime);
        Date end = dateFormat.parse(endTime);

        Calendar startCalendar = Calendar.getInstance();
        Calendar endCalendar = Calendar.getInstance();
        startCalendar.setTime(start);
        endCalendar.setTime(end);

        /**
         * todo
         * 这种所发效率高吗，如果相差一年，要循环300多次才行
         */
        int days = 0;
        while (startCalendar.before(endCalendar)) {
            startCalendar.add(Calendar.DAY_OF_YEAR, 1);
            days += 1;
        }

        return days;
    }

    /**
     * 拿到时间的小时数
     * @param timevalue
     * @return
     * @throws ParseException
     */
    public static int getHourByDate(String timevalue) throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HHmmss");
        Date time = dateFormat.parse(timevalue);
        dateFormat = new SimpleDateFormat("hh");
        String resulthour = dateFormat.format(time);
        return Integer.valueOf(resulthour) ;
    }
}
