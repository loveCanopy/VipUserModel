package com.baidumusic.vipuser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.alibaba.fastjson.JSONObject;

public class Click_ETL {
	private String ip;
	private String remote_user;
	private String date;
	private String time_zone;
	private String method;
	private String request;
	private String http_p_ver;
	private String status;
	private String bbs;
	private String connect;
	private String request_time;
	private String refer;
	private String cookie;
	private String userAgent;
	private boolean valid = true;
	private static final  int  NUM_FIELDS =  14 ;
	private static final String logEntryPattern = "^([\\d.]+) - (\\S+) \\[([\\w:/]+) ([+\\-]\\d{4})\\] \"(\\S+) (.+?) .*TTP/(.*)\" "
			+ "(\\d+) (\\d+) (\\d+) ([\\d.]+) \"(.+?)\" (.+?) \"(.+?)\"";
	
	public void setip(String ip) {
		this.ip = ip;
	}
	
	public String getip() {
		return ip;
	}

	public void setremote_user(String remote_user) {
		this.remote_user = remote_user;
	}
	
	public String getremote_user() {
		return remote_user;
	}

	public void setdate(String date) {
		this.date = date;
	}

	public String getdate() {
		return date;
	}

	public String gettime_zone() {
		return time_zone;
	}

	public void settime_zone(String time_zone) {
		this.time_zone = time_zone;
	}

	public String getmethod() {
		return method;
	}

	public void setmethod(String method) {
		this.method = method;
	}

	public String getrequest() {
		return request;
	}

	public void setrequest(String request) {
		this.request = request;
	}

	public String gethttp_p_ver() {
		return http_p_ver;
	}

	public void sethttp_p_ver(String http_p_ver) {
		this.http_p_ver = http_p_ver;
	}

	public String getstatus() {
		return status;
	}

	public void setstatus(String status) {
		this.status = status;
	}

	public String getbbs() {
		return bbs;
	}

	public void setbbs(String bbs) {
		this.bbs = bbs;
	}

	public String getconnect() {
		return connect;
	}

	public void setconnect(String connect) {
		this.connect = connect;
	}

	public String getrequest_time() {
		return request_time;
	}

	public void setrequest_time(String request_time) {
		this.request_time = request_time;
	}

	public String getrefer() {
		return refer;
	}

	public void setrefer(String refer) {
		this.refer = refer;
	}

	public String getcookie() {
		return cookie;
	}

	public void setcookie(String cookie) {
		this.cookie = cookie;
	}

	public String getuserAgent() {
		return userAgent;
	}

	public void setuserAgent(String userAgent) {
		this.userAgent = userAgent;
	}

	public boolean isValid() {
		return valid;
	}

	public void setValid(boolean valid) {
		this.valid = valid;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("valid:" + this.valid);
		sb.append("\nip:" + this.ip);
		sb.append("\nremote_user:" + this.remote_user);
		sb.append("\ndate:" + Tools.dateToLocal(this.date));
		sb.append("\ntime_zone:" + this.time_zone);
		sb.append("\nmethod:" + this.method);
		sb.append("\nrequest:" + this.request);
		sb.append("\nhttp_p_ver:" + this.http_p_ver);
		sb.append("\nstatus:" + this.status);
		sb.append("\nbbs:" + this.bbs);
		sb.append("\nconnect:" + this.connect);
		sb.append("\nrequest_time:" + this.request_time);
		sb.append("\nrefer:" + this.refer);
		sb.append("\ncookie:" + this.cookie);
		sb.append("\nuserAgent:" + this.userAgent);
		return sb.toString();
	}
	
	public String toStdline() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.ip);
		sb.append("\t" + this.remote_user);
		sb.append("\t" + Tools.dateToLocal(this.date));
		sb.append("\t" + this.time_zone);
		sb.append("\t" + this.method);
		sb.append("\t" + this.request);
		sb.append("\t" + this.http_p_ver);
		sb.append("\t" + this.status);
		sb.append("\t" + this.bbs);
		sb.append("\t" + this.connect);
		sb.append("\t" + this.request_time);
		sb.append("\t" + this.refer);
		sb.append("\t" + this.cookie);
		sb.append("\t" + this.userAgent);
		return sb.toString();
	}

	public static Click_ETL parser(String line) {
		Click_ETL kpi = new Click_ETL();
		if (line == null || line.length() == 0) {
			kpi.setValid(false);
			return kpi;
		}
		Pattern p = Pattern.compile ( logEntryPattern ) ;
		Matcher matcher = p.matcher ( line ) ; 
		if  ( !matcher.matches ()  || NUM_FIELDS != matcher.groupCount ()) {
			kpi.setValid(false); 
		} else {
			kpi.setip (matcher.group ( 1 )) ;
			kpi.setremote_user ( matcher.group ( 2 )) ; 
			kpi.setdate (matcher.group ( 3 )) ;
			kpi.settime_zone (matcher.group ( 4 )) ;
			kpi.setmethod (matcher.group ( 5 )) ;
			kpi.setrequest ( matcher.group ( 6 )) ;
			kpi.sethttp_p_ver (matcher.group ( 7 )) ; 
			kpi.setstatus (matcher.group ( 8 )) ; 
			kpi.setbbs (matcher.group ( 9 )) ; 
			kpi.setconnect (matcher.group ( 10 )) ; 
			kpi.setrequest_time (matcher.group ( 11 )) ; 
			kpi.setrefer (matcher.group ( 12 )) ; 
			kpi.setcookie (matcher.group ( 13 )) ;
			kpi.setuserAgent (matcher.group ( 14 )) ; 
		}
		return kpi;
	}
	
	public static String splitMap(Click_ETL kpi) {
		JSONObject json = new JSONObject();
		String requst = kpi.getrequest();
		if (requst == null || requst.length() == 0)
			return "";
		int index = requst.indexOf("?");
		if (index > 0 && index < requst.length()-1) {
			requst = requst.substring(index+1);
		}
		String[] logList = requst.split("&");
		for (String log : logList) {
			String[] kv = log.split("=");
			if (2 == kv.length) {
				json.put(kv[0], kv[1]);
			}
		}
		return json.toString();
	}
	
	public static String SplitMap(String logStr) {
		JSONObject json = new JSONObject();
		if (logStr == null || logStr.length() == 0)
			return "";
		int index = logStr.indexOf("?");
		if (index > 0 && index < logStr.length()-1) {
			logStr = logStr.substring(index+1);
		}
		String[] logList = logStr.split("&");
		for (String log : logList) {
			String[] kv = log.split("=");
			if (2 == kv.length) {
				json.put(kv[0], kv[1]);
			}
		}
		return json.toString();
	}

	public static String parseLine(String line) {
		Click_ETL kpi = parser(line);
		if (!kpi.isValid()) {
			return "";
		}
		String requst = kpi.getrequest();
		String json = SplitMap(requst);
		String result = kpi.toStdline() + "\t" + json;
		return result;
	}
}