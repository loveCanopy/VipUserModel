package com.baidumusic.vipuser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.alibaba.fastjson.JSONObject;

/*
 * KPI Object
 */
public class Click_ETL_orig {
	private String ip;// 记录客户端的ip地址
	private String remote_user;// 记录客户端用户名称,忽略属性"-"
	private String date;
	private String month;
	private String year;
	private String hour;
	private String minute;
	private String second;
	private String time_zone;
	private String method;
	private String request;// 记录请求的url与http协议
	private String http_p_ver;
	private String status;// 记录请求状态；成功是200
	private String bbs;
	private String connect;
	private String request_time;
	private String refer;// 用来记录从那个页面链接访问过来的
	private String cookie;
	private String userAgent;// 记录客户浏览器的相关信息
	
	private boolean valid = true;// 判断数据是否合法
	public static final  int  NUM_FIELDS =  19 ;
	public static final String logEntryPattern = "^([\\d.]+) - (\\S+) \\[(\\d+)/(\\S+)/(\\d{4}):(\\d+):(\\d+):(\\d+) ([+\\-]\\d{4})\\] \"(\\S+) (.+?) .*TTP/(.*)\" (\\d{3}) (\\d+) (\\d+) ([\\d.]+) \"(.+?)\" (.+?) \"(.+?)\"";
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("valid:" + this.valid);
		sb.append("\nip:" + this.ip);
		sb.append("\nremote_user:" + this.remote_user);
		sb.append("\ndate:" + this.date);
		sb.append("\nmonth:" + this.month);
		sb.append("\nyear:" + this.year);
		sb.append("\nhour:" + this.hour);
		sb.append("\nminute:" + this.minute);
		sb.append("\nsecond:" + this.second);
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
		sb.append("\t" + this.date);
		sb.append("\t" + this.month);
		sb.append("\t" + this.year);
		sb.append("\t" + this.hour);
		sb.append("\t" + this.minute);
		sb.append("\t" + this.second);
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

	public void setmonth(String month) {
		this.month = month;
	}

	public String getmonth() {
		return month;
	}

	public void setyear(String year) {
		this.year = year;
	}

	public String getyear() {
		return year;
	}

	public String gethour() {
		return hour;
	}

	public void sethour(String hour) {
		this.hour = hour;
	}

	public String getminute() {
		return minute;
	}

	public void setminute(String minute) {
		this.minute = minute;
	}

	public String getsecond() {
		return second;
	}

	public void setsecond(String second) {
		this.second = second;
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

	public static Click_ETL_orig parser(String line) {
		Click_ETL_orig kpi = new Click_ETL_orig();
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
			kpi.setmonth (matcher.group ( 4 )) ;
			kpi.setyear (matcher.group ( 5 )) ;
			kpi.sethour (matcher.group ( 6 )) ;
			kpi.setminute (matcher.group ( 7 )) ;
			kpi.setsecond (matcher.group ( 8 )) ;
			kpi.settime_zone (matcher.group ( 9 )) ;
			kpi.setmethod (matcher.group ( 10 )) ;
			kpi.setrequest ( matcher.group ( 11 )) ;
			kpi.sethttp_p_ver (matcher.group ( 12 )) ; 
			kpi.setstatus (matcher.group ( 13 )) ; 
			kpi.setbbs (matcher.group ( 14 )) ; 
			kpi.setconnect (matcher.group ( 15 )) ; 
			kpi.setrequest_time (matcher.group ( 16 )) ; 
			kpi.setrefer (matcher.group ( 17 )) ; 
			kpi.setcookie (matcher.group ( 18 )) ;
			kpi.setuserAgent (matcher.group ( 19 )) ; 
		}
		return kpi;
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

	public static String toResultline(String line) {
		Click_ETL_orig kpi = parser(line);
		String requst = kpi.getrequest();
		String hMap = "";
		if (requst != null && requst.length() > 0) {
			hMap = SplitMap(requst);
		}
		String result = parser(line).toStdline() + "\t" + hMap;
		return result;
	}
	
	public static void main(String args[]) {
		String line = "222.68.172.190 - - [18/Sep/2013:06:49:57 +0000] \"GET /images/my.jpg HTTP/1.1\" 200 19939 \"http://www.angularjs.cn/A00n\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.66 Safari/537.36\"";
		line = "211.87.94.18 - - [29/Aug/2016:10:28:04 +0800] \"GET /v.gif?pid=323&type=exposure&ref=webapp&os=iphone&os_v=9.2&v=1.0.0&r=1472437685112&page=songidx&page_url=http%3A%2F%2Fmusic.baidu.com%2Fsong%2Fs%2F320753641130856305d2b%3Ffm%3Daltg_new3%26autoplay%3Dtrue&page_refer=https%3A%2F%2Fm.baidu.com%2Ffrom%3D1099a%2Fbd_page_type%3D1%2Fssid%3D0%2Fuid%3D0%2Fbaiduid%3D74925C2593BD949B0B027D7650300578%2Fw%3D0_10_%25E5%2584%25BF%25E6%25AD%258C%2Ft%3Dzbios%2Fl%3D1%2Ftc%3Fref%3Dwww_zbios%26pu%3Dsz%25401320_480%252Ccuid%2540FE52322EB2807C056CA3FA2DD1B079E47CB14C419OCRHKMJFGN%252Ccua%25401242_2208_iphone_7.4.0.2_0%252Ccut%2540iPhone7%25252C1_9.2%252Cosname%2540baiduboxapp%252Cctv%25401%252Ccfrom%25401099a%252Ccsrc%2540home_box_txt%252Cta%2540zbios_1_9.2_6_0.0%252Cusm%25405%252Cvmgdb%25400020100228y%26lid%3D3265875802243769938%26order%3D1%26fm%3Dalop%26tj%3Dmoviesongs_1_0_10_l2%26sec%3D14790%26di%3De840b63aad673927%26bdenc%3D1%26tch%3D124.0.0.0.0.0%26nsrc%3DIlPT2AEptyoA_yixCFOxXnANedT62v3IGw3CLCsK1De8mVjte4viZQRAUTz6NzrIBVKwdoTRsx5GxXOh_7Un7hJDq_pns7-r7SvwdsaergCBKMNFutQ02NPZG7Vqpu8bm4ZauQMsM2NJQScenq%26eqid%3D2d52b8bd2148a6001000000457c27f09%26wd%3D%26clk_info%3D%257B%2522srcid%2522%253A%252246165%2522%252C%2522tplname%2522%253A%2522moviesongs%2522%252C%2522t%2522%253A1472364300560%252C%2522xpath%2522%253A%2522div-div-div-div-div-div-div2-div-div2-a-div%2522%257D&channel=&expoitem=ad HTTP/1.1\" 200 0 1281356188 0.000 \"http://music.baidu.com/song/s/320753641130856305d2b?fm=altg_new3&autoplay=true\" BAIDUID=74925C2593BD949B0B027D7650300578:FG=1; BAIDULOC=12844829.221592_3907152.2562618_1000_128_1472364288192; BAIDU_WISE_UID=wapp_1462766783641_492; BDLIGHTID=6776538|1; BIDUPSID=74925C2593BD949B0B027D7650300578; H_PS_PSSID=1449_11572_20857_20732_20837; H_WISE_SIDS=104494_100121_100289_109627_108051_104340_103299_107635_100099_108020_108411_108458_108364_108334_107694_107503_108085_107311_109204_109531_109506_109558_107806_109638_108075_108013_108297_108340_109542_109524; PSTM=1448382199; WISE_HIS_PM=0; locale=zh \"Mozilla/5.0 (iPhone; CPU iPhone OS 9_2 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/13C75 search%2F1.0 baiduboxapp/0_2.0.4.7_enohpi_8022_2421/2.9_1C2%257enohPi/1099a/FE52322EB2807C056CA3FA2DD1B079E47CB14C419OCRHKMJFGN/1\"";
		line = "183.43.146.174 - - [29/Aug/2016:15:09:56 +0800] \"llapp&ref=webapp&os=android&os_v=5.1.1&v=1.0.0&r=1472454595545&page=undefined&page_url=http%3A%2F%2Fmusic.baidu.com%2Fsong%2F267453821%3Ffm%3Daltg_new3%26autoplay%3Dtrue&page_refer=http%3A%2F%2Fm.baidu.com%2Ffrom%3D844b%2Fbd_page_type%3D1%2Fssid%3D0%2Fuid%3D0%2Fpu%3Dusm%25404%252Csz%25401320_1001%252Cta%2540iphone_2_5.1_3_537%2Fbaiduid%3D6D7AC3C628C163708E74D471232CB72F%2Fw%3D0_10_%25E8%25BF%2598%25E9%25AD%2582%25E4%25BB%25AC%2Ft%3Diphone%2Fl%3D1%2Ftc%3Fref%3Dwww_iphone%26lid%3D7799828458995988513%26order%3D1%26fm%3Dalop%26tj%3D2c9_1_0_10_b1%26sec%3D14815%26di%3Dee98540fce088753%26bdenc%3D1%26tch%3D124.173.331.202.3.960%26nsrc%3DIlPT2AEptyoA_yixCFOxXnANedT62v3IGw3CLCsK1De8mVjte4viZQRAUTz6NzrIBZOfczLLshUDwWGcWTBrzB1vf4AkhER6jjbagPrusdD_UdRFrcp6O0LWWjUh%26eqid%3D6c3e8ea87f7038001000000657c3de83%26wd%3D%26clk_info%3D%257B%2522srcid%2522%253A%25228041%2522%252C%2522tplname%2522%253A%2522musicsongs%2522%252C%2522t%2522%253A1472454286781%252C%2522xpath%2522%253A%2522div-div-div-div-div-div4-a%2522%257D&channel=&iss\" 400 172 1284224776 0.406 \"-\" - \"-\"";
		line = "182.111.199.134 - - [29/Aug/2016:12:57:16 +0800] \"GET /v.gif?pid=323&type=exposure&ref=webapp&os=android&os_v=5.0.2&v=1.0.0&r=1472446636756&page=songidx&page_url=http%3A%2F%2Fmusic.baidu.com%2Fsong%2Fs%2F43061e8f1a08564d81e3%3Ffm%3Daltg_new3%26autoplay%3Dtrue&page_refer=https%3A%2F%2Fm.baidu.com%2Ffrom%3D1013755q%2Fbd_page_type%3D1%2Fssid%3D0%2Fuid%3D0%2Fpu%3Dusm%25403%252Csz%25401320_1001%252Cta%2540iphone_2_5.0_3_537%2Fbaiduid%3DA337004D02B455649F0C625F6B1DF519%2Fw%3D0_10_%25E7%25BB%258F%25E5%2585%25B8%25E5%2584%25BF%25E6%25AD%258C%2Ft%3Diphone%2Fl%3D1%2Ftc%3Fref%3Dwww_iphone%26lid%3D16156880463870515853%26order%3D1%26fm%3Dalop%26tj%3Dmoviesongs_1_0_10_b4%26sec%3D14812%26di%3D2227512b8486dec5%26bdenc%3D1%26tch%3D124.0.0.0.0.0%26nsrc%3DIlPT2AEptyoA_yixCFOxXnANedT62v3IGw3CLCsK1De8mVjte4viZQRAUTz6NzrIBVKwdoTKsh5HwSGjOWV87Rp3qqg5q7pq7Gja9aapdhLqT1hNq_wq0cWVDXBu54nakud7xBtoReJ-Ry9u%26eqid%3De038c52dd77358001000000257c3c063%26wd%3D%26clk_info%3D%257B%2522srcid%2522%253A%252246165%2522%252C%2522tplname%2522%253A%2522moviesongs%2522%252C%2522t%2522%253A1472446604163%252C%2522xpath%2522%253A%2522div-div-div-div-div-div-div4-div-div3-a-span%2522%257D&channel=&expoitem=ad HTTP/1.1\" 200 0 1336500264 0.000 \"http://music.baidu.com/song/s/43061e8f1a08564d81e3?fm=altg_new3&autoplay=true\" sbapp=2.0.1; BIDUPSID=A337004D02B455649F0C625F6B1DF519; PSTM=1456994981; PLUS=0; plus_cv=1::m:e0d701e5; BCLID=15638184122396049851; BDSFRCVID=JjFsJeC62w9FknjRkFrmUnZkFeKaos5TH6aGaFPg7StDynbkmxSeEG0PfOlQpYD-MDWeogKKQgOTHRnP; H_BDCLCKID_SF=JbFD_D--fCv5eJCkKtr_bn0thxtXKtusWDQlQhcH0KLKfITDMPT804PzKxcP2qTW3DTi3R7bJxb1MRjj-fjmbT-95-O82tjWa6bg0l5TtUJt8nRhKhrUqfuv3UoyKMniLCv9-pn4aft0HPonHjDaDjjP; H_WISE_SIDS=106473_106305_100043_102479_102629_109627_108045_103569_109639_107635_104340_108051_108912_109654_108374_108458_108411_108333_107694_107502_108084_109532_109505_109497_107806_108012_108295_109666_109485_107315_107242_100458; BAIDUID=A337004D02B455649F0C625F6B1DF519:FG=1 \"Mozilla/5.0 (Linux; Android 5.0.2; SAMSUNG SM-A5000 Build/LRX22G) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/3.3 Chrome/38.0.2125.102 Mobile Safari/537.36\"";
		System.out.println(line);
		System.out.println(Click_ETL_orig.parser(line).toString());
		System.out.println(toResultline(line));
	}
}