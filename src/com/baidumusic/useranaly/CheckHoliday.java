package com.baidumusic.useranaly;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import com.baidumusic.vipuser.Tools;

public class CheckHoliday {
	private static List<Calendar> holidayList = new ArrayList<Calendar>();
	private static List<Calendar> weekendList = new ArrayList<Calendar>();
	private static final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	/**
	 * @return return boolean true means holiday, else return false throws
	 */
	public static boolean checkHoliday(String date) throws Exception {
		Calendar calendar = Calendar.getInstance();
		Date d = df.parse(Tools.formatDate2(date, true));
		calendar.setTime(d);

		// Is weekend?
		if (calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY
				|| calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY) {

			// Is holiday?
			for (Calendar ca : weekendList) {
				if (ca.get(Calendar.MONTH) == calendar.get(Calendar.MONTH)
						&& ca.get(Calendar.DAY_OF_MONTH) == calendar.get(Calendar.DAY_OF_MONTH)
						&& ca.get(Calendar.YEAR) == calendar.get(Calendar.YEAR)) {
					return false;
				}
			}

			return true;
		}
		// Whether holiday?
		for (Calendar ca : holidayList) {
			if (ca.get(Calendar.MONTH) == calendar.get(Calendar.MONTH)
					&& ca.get(Calendar.DAY_OF_MONTH) == calendar.get(Calendar.DAY_OF_MONTH)
					&& ca.get(Calendar.YEAR) == calendar.get(Calendar.YEAR)) {
				return true;
			}
		}

		return false;
	}

	/**
	 * 
	 * Put all holidays into list
	 * 
	 * @param date
	 *            format: 2016-05-09 return void type throws
	 */
	public void initHolidayList(String date) {
		String[] da = date.split("-");

		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.YEAR, Integer.valueOf(da[0]));
		calendar.set(Calendar.MONTH, Integer.valueOf(da[1]) - 1);// 月份比正常小1,0代表一月
		calendar.set(Calendar.DAY_OF_MONTH, Integer.valueOf(da[2]));
		holidayList.add(calendar);
	}

	/**
	 * Weekends which are work days
	 */
	public void initWeekendList(String date) {
		String[] da = date.split("-");

		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.YEAR, Integer.valueOf(da[0]));
		calendar.set(Calendar.MONTH, Integer.valueOf(da[1]) - 1);// 月份比正常小1,0代表一月
		calendar.set(Calendar.DAY_OF_MONTH, Integer.valueOf(da[2]));
		weekendList.add(calendar);
	}
	
	public static void init() {
		CheckHoliday ct = new CheckHoliday();
		// ---------------init holidays------------------
		ct.initHolidayList("2016-01-01");
		ct.initHolidayList("2016-01-02");
		ct.initHolidayList("2016-01-03");

		ct.initHolidayList("2016-02-07");
		ct.initHolidayList("2016-02-08");
		ct.initHolidayList("2016-02-09");
		ct.initHolidayList("2016-02-10");
		ct.initHolidayList("2016-02-11");
		ct.initHolidayList("2016-02-12");
		ct.initHolidayList("2016-02-13");

		ct.initHolidayList("2016-04-02");
		ct.initHolidayList("2016-04-03");
		ct.initHolidayList("2016-04-04");

		ct.initHolidayList("2016-04-30");
		ct.initHolidayList("2016-05-01");
		ct.initHolidayList("2016-05-02");

		ct.initHolidayList("2016-06-09");
		ct.initHolidayList("2016-06-10");
		ct.initHolidayList("2016-06-11");

		ct.initHolidayList("2016-09-15");
		ct.initHolidayList("2016-09-16");
		ct.initHolidayList("2016-09-17");

		ct.initHolidayList("2016-10-01");
		ct.initHolidayList("2016-10-02");
		ct.initHolidayList("2016-10-03");
		ct.initHolidayList("2016-10-04");
		ct.initHolidayList("2016-10-05");
		ct.initHolidayList("2016-10-06");
		ct.initHolidayList("2016-10-07");

		// -------------- delete working weekend days---------------------
		ct.initWeekendList("2016-02-06");
		ct.initWeekendList("2016-02-14");
		ct.initWeekendList("2016-05-02");
		ct.initWeekendList("2016-06-12");
		ct.initWeekendList("2016-09-18");
		ct.initWeekendList("2016-10-08");
		ct.initWeekendList("2016-10-09");
	}
	
	static {
		init();
	}

	public static void main(String[] args) throws Exception {
		String date = "2016-11-11 10:10:10";
		System.out.println(CheckHoliday.checkHoliday(date));

	}
}