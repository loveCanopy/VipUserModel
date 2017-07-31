package com.baidumusic.vipuser;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.alibaba.fastjson.JSONObject;
import com.google.common.net.InternetDomainName;

public class Tools {
	public static String COMMA = ",";
	public static String SPACE = " ";
	public static final String PLACEHOLDER = "0";

	/**
	 * get value of given @fieldName from log @line
	 * 
	 * @param fieldName
	 *            whole name of field such as "&hid="
	 * @param line
	 * @return
	 */
	public static String getValue(String fieldName, String line) {
		String value = "";
		int index = line.indexOf(fieldName);
		if (index > 0) {
			int endIndex = line.indexOf("&", index + fieldName.length());
			if (endIndex > 0) {
				value = line.substring(index + fieldName.length(), endIndex);
			} else {
				endIndex = line.indexOf(" ", index + fieldName.length());
				if (endIndex > 0) {
					value = line.substring(index + fieldName.length(), endIndex);
				}
			}
		}
		return value;
	}

	public static Map<String, String> months = new HashMap<String, String>(12);
	static {
		months.put("Jan", "01");
		months.put("Feb", "02");
		months.put("Mar", "03");
		months.put("Apr", "04");
		months.put("May", "05");
		months.put("Jun", "06");
		months.put("Jul", "07");
		months.put("Aug", "08");
		months.put("Sep", "09");
		months.put("Oct", "10");
		months.put("Nov", "11");
		months.put("Dec", "12");
	}

	/**
	 * format given @time from "[03/Sep/2012:00:00:19" to "YYYYmmdd:HH:MM:SS"
	 * 
	 * @param time
	 *            picked from log which is like "[03/Sep/2012:00:00:19"
	 * @param withHour
	 *            if true, "HH:MM:SS" is returned along with "YYYYmmdd"
	 * @return
	 */
	public static String formatDate(String time, boolean withHour) {
		if (time.matches("[0-9]{2}/[a-zA-Z]+/[0-9]{4}:[0-9]{2}:[0-9]{2}:[0-9]{2}")
				|| time.matches("\\[([\\w:/]+\\s[+\\-]\\d{4})\\]")) {
			String[] parts = time.split(":", 2);// date and time
			String month = parts[0].split("/", 3)[1];
			String year = parts[0].split("/", 3)[2];
			String day = parts[0].split("/", 3)[0].replace("[", "");
			return (year + "-" + months.get(month) + "-" + day) + (withHour ? " " + parts[1].split(" ")[0] : "");
		}
		return "";
	}
	
	public static String dateToLocal(String date) {
		if (date.matches("\\[([\\w:/]+\\s[+\\-]\\d{4})\\]")) {
			date = date.split("\\s")[0].substring(1);
		}
		String pattern = "dd/MMM/yyyy:HH:mm:ss";
		String tar_pat = "yyyy-MM-dd HH:mm:ss";
		SimpleDateFormat sour_sdf = new SimpleDateFormat(pattern, Locale.US);
		SimpleDateFormat tar_sdf = new SimpleDateFormat(tar_pat, Locale.US);
		try {
			return tar_sdf.format(sour_sdf.parse(date));
		} catch (ParseException localParseException) {
		}
		return "";
	}

	/**
	 * format given @time from "[03/Sep/2012:00:00:19" to "YYYYmmdd"
	 * 
	 * @param time
	 *            picked from log which is like "[03/Sep/2012:00:00:19"
	 * @return
	 */
	public static String formatDate(String time) {
		return formatDate(time, false);
	}

	/**
	 * iterate @set1 to check how many of them exists in @set2
	 * 
	 * @param set1
	 * @param set2
	 * @return
	 */
	public static int getSetIntersection(Set<String> set1, Set<String> set2) {
		int count = 0;
		Iterator<String> it = set1.iterator();
		while (it.hasNext()) {
			if (set2.contains(it.next())) {
				count++;
			}
		}
		return count;
	}
	
	public static byte[] toByteArray(int iSource, int iArrayLen) {
	    byte[] bLocalArr = new byte[iArrayLen];
	    for (int i = 0; (i < 4) && (i < iArrayLen); i++) {
	        bLocalArr[i] = (byte) (iSource >> 8 * i & 0xFF);
	    }
	    return bLocalArr;
	}

	public static long BytetoLong(byte[] bRefArr) {
	    long iOutcome = 0;
	    byte bLoop;

	    for (int i = 0; i < bRefArr.length; i++) {
	        bLoop = bRefArr[i];
	        iOutcome += (bLoop & 0xFF) << (8 * i);
	    }
	    return iOutcome;
	}
	
	public static String filterCol(int col, String todel) {
		String res = "";
		String[] nums = todel.split(",");
		HashSet<Integer> hSet = new HashSet<Integer>();
		for (String n : nums) {
			hSet.add(Integer.valueOf(n));
		}
		for (int i=0; i<col-1; i++) {
			if (!hSet.contains(i)) {
				res += i + ",";
			}
		}
		return res;
	}
	
	public static JSONObject splitMap(String request) {
		JSONObject json = new JSONObject();
		if (request == null || request.length() == 0)
			return json;
		int index = request.indexOf("?");
		if (index > 0 && index < request.length()-1) {
			request = request.substring(index+1);
		}
		String[] logList = request.split("&");
		for (String log : logList) {
			String[] kv = log.split("=");
			if (2 == kv.length) {
				json.put(kv[0], kv[1]);
			}
		}
		return json;
	}
	
	public static String str2stamp(String user_time, String DATE_FORMAT) {
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        String re_time = null;
        Date d;
        try {
            d = sdf.parse(user_time);
            long l = d.getTime();
            String str = String.valueOf(l);
            re_time = str.substring(0, 10);
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return re_time;
    }
	
	public static String stamp2str2(String cc_time) {//2016-02-27 20:56:51
        String re_StrTime = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long lcc_time = Long.valueOf(cc_time);
        re_StrTime = sdf.format(new Date(lcc_time * 1000L));
        return re_StrTime;
    }

	public static String formatDate2(String time, boolean withHour) {
		if (time.matches("[0-9]{4}-[a-zA-Z]+-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}")) {
			String[] parts = time.split(" ", 2);// date and time
			String year = parts[0].split("-", 3)[0];
			String month = parts[0].split("-", 3)[1];
			String day = parts[0].split("-", 3)[2];
			return (year + "-" + months.get(month) + "-" + day) + (withHour ? " " + parts[1].split(" ")[0] : "");
		}
		return time;
	}
	
	public static String getTopPrivateDomain(final String domain) {
		String result = "";
		try {
			final InternetDomainName from = InternetDomainName.from(domain);
			final InternetDomainName topPrivateDomain = from.topPrivateDomain();
			result = topPrivateDomain.name();
		} catch (Exception e) {
			result = "";
		}
		return result;
	}
	
	public static void main(String[] args) throws IOException {
		// [18/Sep/2013:06:49:57 +0000]
		System.out.println(formatDate("18/Sep/2013:06:49:57", true));
		System.out.println(formatDate("[18/Sep/2013:06:49:57 +0000]", true));
		
		System.out.println(dateToLocal("18/Sep/2013:06:49:57"));
		System.out.println(dateToLocal("[18/Sep/2013:06:49:57 +0000]"));
		System.out.println(stamp2str2("1475077273"));
		
		System.out.println("there.. " + formatDate2("2016-Sep-01 20:12:17", true));
		
		System.out.println(getTopPrivateDomain("www.baidu.com"));
	}
}
