package com.baidumusic.vipuser;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StatFunc {
	public final static String TAB = "\t";
	public final static DecimalFormat df = new DecimalFormat("0.000000");

	public static HashMap<String, Integer> AddMap(HashMap<String, Integer> map, String key) {
		if (map.containsKey(key)) {
			int cnt = map.get(key);
			map.put(key, cnt + 1);
		} else {
			map.put(key, 1);
		}
		return map;
	}

	public static String getStat(HashMap<String, Integer> hMap) {
		ArrayList<Integer> aList = new ArrayList<Integer>();
		Iterator<Integer> it = hMap.values().iterator();
		while (it.hasNext()) {
			int val = (int) it.next();
			aList.add(val);
		}
		Integer[] arr = new Integer[aList.size()];
		aList.toArray(arr);
		String stat = MathStatics.CombineStat(arr);
		return stat;
	}

	public static int getSingleRate(HashMap<String, Integer> hMap, String size) {
		if (hMap.containsKey(size)) {
			return hMap.get(size);
		} else {
			return 0;
		}
	}

	public static String formatRate(String rate) {
		String res = "000";
		if (!rate.matches("[0-9]+")) {
			return res;
		}
		int irate = Integer.valueOf(rate);
		if (irate <= 0) {
			return res;
		}
		if (irate < 32 || irate < (32 + 64) / 2) {
			res = "32";
		} else if (irate < 64 || irate < (64 + 96) / 2) {
			res = "64";
		} else if (irate < 96 || irate < (96 + 128) / 2) {
			res = "96";
		} else if (irate < 128 || irate < (128 + 256) / 2) {
			res = "128";
		} else if (irate < 256 || irate < (256 + 320) / 2) {
			res = "256";
		} else if (irate < 320 || irate < (320 + 500) / 2) {
			res = "320";
		} else {
			res = "320plus";
		}

		return res;
	}

	public static String getRate(HashMap<String, Integer> hMap) {
		String rate = "0\t0\t0\t0\t0\t0\t0\t0";
		if (hMap.size() == 0) {
			return rate;
		}
		int r_000 = getSingleRate(hMap, "000");
		int r_32 = getSingleRate(hMap, "32");
		int r_64 = getSingleRate(hMap, "64");
		int r_96 = getSingleRate(hMap, "96");
		int r_128 = getSingleRate(hMap, "128");
		int r_256 = getSingleRate(hMap, "256");
		int r_320 = getSingleRate(hMap, "320");
		int r_320plus = getSingleRate(hMap, "320plus");
		return r_000 + TAB + r_32 + TAB + r_64 + TAB + r_96 + TAB + r_128 + TAB + r_256 + TAB + r_320 + TAB + r_320plus;
	}

	public static String getPlaytime(HashMap<String, Integer> hMap) {
		String time = "0\t0\t0\t0";
		if (hMap.size() == 0) {
			return time;
		}
		int p100ms = hMap.containsKey("p100ms") ? hMap.get("p100ms") : 0;
		int p60s = hMap.containsKey("p60s") ? hMap.get("p60s") : 0;
		int pend = hMap.containsKey("pend") ? hMap.get("pend") : 0;
		int sum = p100ms + p60s + pend;
		sum = (sum == 0) ? 1 : sum;
		return df.format(p100ms * 1.0 / sum) + TAB + df.format(p60s * 1.0 / sum) + TAB + df.format(pend * 1.0 / sum)
				+ TAB + df.format(p100ms == 0 ? 0 : p60s * 1.0 / p100ms);
	}

	public static int getDay(String date) {
		String pattern = "^\\d{4}-\\d{2}-(\\d{2}) (\\d{2}):\\d{2}:\\d{2}$";
		Pattern p = Pattern.compile(pattern);
		Matcher matcher = p.matcher(date);
		if (matcher.matches()) {
			return Integer.valueOf(matcher.group(1));
		}

		return 0;
	}

	public static int getHour(String date) { // 2016-10-13 00:00:00
		String pattern = "^\\d{4}-\\d{2}-(\\d{2}) (\\d{2}):\\d{2}:\\d{2}$";
		Pattern p = Pattern.compile(pattern);
		Matcher matcher = p.matcher(date);
		if (matcher.matches()) {
			return Integer.valueOf(matcher.group(2));
		}

		return 24;
	}

	public static HashMap<String, Integer> viptypeMap = new HashMap<String, Integer>();
	static {
		viptypeMap.put("no_login", 1);
		viptypeMap.put("no_login,no_order", 2);
		viptypeMap.put("no_order", 3);
		viptypeMap.put("vip_order", 4);
		viptypeMap.put("no_login,vip_order", 4);
		viptypeMap.put("no_order,vip_order", 4);
		viptypeMap.put("no_login,no_order,vip_order", 4);
		viptypeMap.put("*vip_order*", 5);
		viptypeMap.put("other", 6);
	}

	public static int getVipType(String type) {
		int res = 1;
		if (type.equals("no_login")) {
			res = viptypeMap.get("no_login");
		} else if (type.equals("no_login,no_order")) {
			res = viptypeMap.get("no_login,no_order");
		} else if (type.equals("no_order")) {
			res = viptypeMap.get("no_order");
		} else if (type.equals("no_login,vip_order")) {
			res = viptypeMap.get("no_login,vip_order");
		} else if (type.equals("no_order,vip_order")) {
			res = viptypeMap.get("no_order,vip_order");
		} else if (type.equals("no_login,no_order,vip_order")) {
			res = viptypeMap.get("no_login,no_order,vip_order");
		} else if (type.contains("vip_order")) {
			res = viptypeMap.get("*vip_order*");
		} else {
			res = viptypeMap.get("other");
		}
		return res;
	}

	public static HashMap<String, Integer> shoumaitypeMap = new HashMap<String, Integer>();
	static {
		shoumaitypeMap.put("no_login", 1);
		shoumaitypeMap.put("no_login,no_order", 2);
		shoumaitypeMap.put("no_order", 3);
		shoumaitypeMap.put("shoumai", 4);
		shoumaitypeMap.put("no_login,shoumai", 4);
		shoumaitypeMap.put("no_order,shoumai", 4);
		shoumaitypeMap.put("no_login,no_order,shoumai", 4);
		shoumaitypeMap.put("*shoumai*", 5);
		shoumaitypeMap.put("other", 6);
		
	}

	public static int getShoumaiType(String type) {
		int res = 1;
		if (type.equals("no_login")) {
			res = shoumaitypeMap.get("no_login");
		} else if (type.equals("no_login,no_order")) {
			res = shoumaitypeMap.get("no_login,no_order");
		} else if (type.equals("no_order")) {
			res = shoumaitypeMap.get("no_order");
		}  else if (type.equals("no_login,shoumai")) {
			res = shoumaitypeMap.get("no_login,shoumai");
		} else if (type.equals("no_order,shoumai")) {
			res = shoumaitypeMap.get("no_order,shoumai");
		} else if (type.equals("no_login,no_order,shoumai")) {
			res = shoumaitypeMap.get("no_login,no_order,shoumai");
		} else if (type.contains("shoumai")) {
			res = shoumaitypeMap.get("*shoumai*");
		} else {
			res = shoumaitypeMap.get("other");
		}
		return res;
	}

	public static String formatPublishtime(String publishtime) {
		String res = "1900";
		if (!publishtime.matches("[0-9]+")) {
			return res;
		}
		int ipt = Integer.valueOf(publishtime);
		if (ipt < 1949) {
			res = "1949";
		} else if (ipt < 1960) {
			res = "1960";
		} else if (ipt < 1970) {
			res = "1970";
		} else if (ipt < 1980) {
			res = "1980";
		} else if (ipt < 1990) {
			res = "1990";
		} else if (ipt < 2000) {
			res = "2000";
		} else if (ipt < 2010) {
			res = "2010";
		} else if (ipt < 2017) {
			res = "2017";
		} else {
			res = "other";
		}
		
		return res;
	}

	public static String getPublishtime(HashMap<String, Integer> hMap) {
		String rate = "0\t0\t0\t0\t0\t0\t0\t0\t";
		if (hMap.size() == 0) {
			return rate;
		}
		int y_1949 = getSingleRate(hMap, "1949");
		int y_1960 = getSingleRate(hMap, "1960");
		int y_1970 = getSingleRate(hMap, "1970");
		int y_1980 = getSingleRate(hMap, "1980");
		int y_1990 = getSingleRate(hMap, "1990");
		int y_2000 = getSingleRate(hMap, "2000");
		int y_2010 = getSingleRate(hMap, "2010");
		int y_2017 = getSingleRate(hMap, "2017");
		int y_other = getSingleRate(hMap, "other");
		return y_1949 + TAB + y_1960 + TAB + y_1970 + TAB + y_1980 + TAB + y_1990 + TAB + y_2000 + TAB + y_2010 + TAB
				+ y_2017 + TAB + y_other;
	}

	public static String getTwoDay(String sj1, String sj2) {
		SimpleDateFormat myFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long day = 0;
		try {
			Date date = myFormatter.parse(sj1);
			Date mydate = myFormatter.parse(sj2);
			day = (date.getTime() - mydate.getTime()) / (24 * 60 * 60 * 1000);
		} catch (Exception e) {
			return "0";
		}
		if (day <= 0) {
			return "0";
		}
		return day + "";
	}
	
	public static StringBuilder Array2SB(int[] arr) {
		StringBuilder sb = new StringBuilder();
		int cnt = 0;
		for (int h : arr) {
			if (cnt++ == 0) {
				sb.append(h);
				continue;
			}
			sb.append(TAB).append(h);
		}
		return sb;
	}

	public static void main(String[] args) {
		System.out.println(formatRate("500"));
		System.out.println(df.format(1));
		
		HashMap<String, Integer> hMap = new HashMap<String, Integer>();
		hMap.put("123", 1);
		System.out.println(getStat(hMap));
		System.out.println(hMap.toString());
	}

}
