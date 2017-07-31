package com.baidumusic.useranaly;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;
import java.util.Map.Entry;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.baidumusic.useranaly.CheckHoliday;
import com.baidumusic.vipuser.MathStatics;
import com.baidumusic.vipuser.StatFunc;
import com.baidumusic.vipuser.Tools;

public class ReadFrFile {
	private static final DecimalFormat df = new DecimalFormat("0.000000");
	private static final DateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static void readTxtFile(String filePath) {
		try {
			String encoding = "UTF-8";
			File file = new File(filePath);
			String TAB = "\t";
			if (file.isFile() && file.exists()) { // 判断文件是否存在
				InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);// 考虑到编码格式
				BufferedReader bufferedReader = new BufferedReader(read);

				HashMap<String, Integer> songMap = new HashMap<String, Integer>();
				HashMap<String, Integer> typeMap = new HashMap<String, Integer>();
				HashMap<String, Integer> playtimeMap = new HashMap<String, Integer>();
				HashMap<String, Integer> styleMap = new HashMap<String, Integer>();
				HashMap<String, Integer> langMap = new HashMap<String, Integer>();
				HashMap<String, Integer> rateMap = new HashMap<String, Integer>();
				HashMap<String, Integer> singerMap = new HashMap<String, Integer>();
				HashMap<String, Integer> publishMap = new HashMap<String, Integer>();
				HashMap<String, Integer> categoryMap = new HashMap<String, Integer>();
				HashMap<String, Integer> filmMap = new HashMap<String, Integer>();
				HashMap<String, Integer> referMap = new HashMap<String, Integer>();

				String play_first_seen = "9999-99-99 99:99:99";
				String play_last_seen = "0000-00-00 00:00:00";
				int[] play_hourArr = new int[24];
				int[] play_dayArr = new int[31];
				int[] play_weekArr = new int[7];
				long play_holiday = 0;
				long play_workday = 0;

				String scan_first_seen = "9999-99-99 99:99:99";
				String scan_last_seen = "0000-00-00 00:00:00";
				int[] scan_hourArr = new int[24];
				int[] scan_dayArr = new int[31];
				int[] scan_weekArr = new int[7];
				long scan_holiday = 0;
				long scan_workday = 0;

				long lt1min = 0;
				long gt1min = 0;

				StringBuilder play_hourSB = new StringBuilder();
				StringBuilder play_daySB = new StringBuilder();
				StringBuilder play_weekSB = new StringBuilder();
				StringBuilder scan_hourSB = new StringBuilder();
				StringBuilder scan_daySB = new StringBuilder();
				StringBuilder scan_weekSB = new StringBuilder();

				long favor_cnt = 0;
				ArrayList<Integer> aList = new ArrayList<Integer>();
				String lineTxt = null;
				String baiduid = null;
				while ((lineTxt = bufferedReader.readLine()) != null) {
					int idx = lineTxt.indexOf(TAB);
					if (idx > 0) {
						baiduid = lineTxt.substring(0, idx);
						String other = lineTxt.substring(idx + 1);

						String[] vparts = other.split(TAB);
						if (9 != vparts.length)
							continue;
						String song_id = vparts[0];
						String play_json = vparts[1];
						String order_info = vparts[2];
						String singer_id = vparts[3];
						String style = vparts[4];
						String language = vparts[5];
						String publish_time = vparts[6];
						String category = vparts[7];
						String si_has_filmtv = vparts[8];

						if (!play_json.matches("\\{.*:.*\\}"))
							continue;

						// songs
						songMap = AddMap(songMap, song_id);

						// user type
						TreeSet<String> typeSet = new TreeSet<String>();
						String[] ods_part = order_info.split(";");
						for (String ods : ods_part) {
							typeSet.add(ods.split("\\|")[0]);
						}
						String user_type = "";
						user_type = typeSet.toString().replace(" ", "");
						user_type = user_type.substring(1, user_type.length() - 1);
						typeMap = AddMap(typeMap, user_type);

						// singers
						singerMap = AddMap(singerMap, singer_id);

						// style
						styleMap = AddMap(styleMap, style);

						// language
						langMap = AddMap(langMap, language.length() == 0 ? "unknown" : language);

						// publish_time
						String publish_year = StatFunc.formatPublishtime(publish_time.split("-")[0]);
						publishMap = AddMap(publishMap, publish_year);

						// category
						categoryMap = AddMap(categoryMap, category);

						// si_has_filmtv
						filmMap = AddMap(filmMap, si_has_filmtv);

						JSONObject play = JSON.parseObject(play_json);
						for (Entry<String, Object> entry : play.entrySet()) {
							String date = Tools.formatDate2(entry.getKey(), true);
							if (date.compareTo(play_first_seen) < 0) {
								play_first_seen = date;
							}
							if (date.compareTo(play_last_seen) > 0) {
								play_last_seen = date;
							}

							Calendar cal = Calendar.getInstance();
							Date d = new Date();
							try {
								d = dateformat.parse(date);
							} catch (ParseException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							cal.setTime(d);
							int hour = cal.get(Calendar.HOUR_OF_DAY);
							int week = cal.get(Calendar.DAY_OF_WEEK) - 1;
							int day = cal.get(Calendar.DAY_OF_MONTH) - 1;
							try {
								if (CheckHoliday.checkHoliday(date)) {
									play_holiday += 1;
								} else {
									play_workday += 1;
								}
							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							play_hourArr[hour] = play_hourArr[hour] + 1;
							play_dayArr[day] = play_dayArr[day] + 1;
							play_weekArr[week] = play_weekArr[week] + 1;

							String getValue = entry.getValue().toString();
							String[] arr = getValue.split(TAB);
							if (10 != arr.length)
								continue;
							String p100ms = arr[2];
							String p60s = arr[3];
							String pend = arr[4];
							String time = arr[5];
							String favor = arr[6];
							String rate = StatFunc.formatRate(arr[8]);

							String ptime = p100ms.equals("1") ? "p100ms" : (p60s.equals("1") ? "p60s" : "pend");
							playtimeMap = StatFunc.AddMap(playtimeMap, ptime);

							if (pend.equals("1")) {
								if (Integer.valueOf(time) > 120000) {
									gt1min += 1;
								} else {
									lt1min += 1;
								}
							} else if (p60s.equals("1")) {
								gt1min += 1;
							}

							if (favor.equals("1")) {
								favor_cnt += 1;
							}

							rateMap = StatFunc.AddMap(rateMap, rate);
						}
					}
				}
				
				String song_stat = StatFunc.getStat(songMap);
				String play_time = StatFunc.getPlaytime(playtimeMap);
				String style_stat = StatFunc.getStat(styleMap);
				String lang_stat = StatFunc.getStat(langMap);
				String rate_stat = StatFunc.getRate(rateMap);
				String singer_stat = StatFunc.getStat(singerMap);
				String publishtime_stat = StatFunc.getPublishtime(publishMap);

				int putong = categoryMap.containsKey("0") ? categoryMap.get("0") : 0;
				int erge = categoryMap.containsKey("1") ? categoryMap.get("1") : 0;
				int duwu = categoryMap.containsKey("2") ? categoryMap.get("2") : 0;
				int cate_all = putong + erge + duwu;
				cate_all = (cate_all == 0) ? 1 : cate_all;
				String category = df.format(putong * 1.0 / cate_all) + TAB + df.format(erge * 1.0 / cate_all) + TAB
						+ df.format(duwu * 1.0 / cate_all);

				int is_film = filmMap.containsKey("1") ? filmMap.get("1") : 0;
				int no_film = filmMap.containsKey("0") ? filmMap.get("0") : 0;
				int film_all = is_film + no_film;
				film_all = (film_all == 0) ? 1 : film_all;
				String film = df.format(is_film * 1.0 / film_all) + TAB + df.format(no_film * 1.0 / film_all);

				play_hourSB = StatFunc.Array2SB(play_hourArr);
				play_daySB = StatFunc.Array2SB(play_dayArr);
				play_weekSB = StatFunc.Array2SB(play_weekArr);

				scan_hourSB = StatFunc.Array2SB(scan_hourArr);
				scan_daySB = StatFunc.Array2SB(scan_dayArr);
				scan_weekSB = StatFunc.Array2SB(scan_weekArr);

				long play_all_day = play_holiday + play_workday;
				play_all_day = (play_all_day == 0) ? 1 : play_all_day;
				play_workday = (play_workday == 0) ? 1 : play_workday;
				String play_work_holiy = df.format(play_holiday * 1.0 / play_all_day) + TAB
						+ df.format(play_workday * 1.0 / play_all_day) + TAB + df.format(play_holiday * 1.0 / play_workday);

				long scan_all_day = scan_holiday + scan_workday;
				scan_all_day = (scan_all_day == 0) ? 1 : scan_all_day;
				scan_workday = (scan_workday == 0) ? 1 : scan_workday;
				String scan_work_holiy = df.format(scan_holiday * 1.0 / scan_all_day) + TAB
						+ df.format(scan_workday * 1.0 / scan_all_day) + TAB + df.format(scan_holiday * 1.0 / scan_workday);

				TreeSet<String> typeSet = new TreeSet<String>();
				int no_login = 0;
				int no_order = 0;
				int other = 0;
				if (typeMap.size() == 0) {
					typeMap.put("no_login", 1);
				}
				
				for (Map.Entry<String, Integer> entry : typeMap.entrySet()) {
					String type = entry.getKey();
					int cnt = entry.getValue();
					typeSet.add(type);
					if (type.equals("no_login")) {
						no_login += cnt;
					} else if (type.equals("no_order")) {
						no_order += cnt;
					} else {
						other += cnt;
					}
				}
				int sum = no_login + no_order + other;
				String rate_type = df.format(sum == 0 ? 1 : no_login * 1.0 / sum) + TAB
						+ df.format(sum == 0 ? 1 : no_order * 1.0 / sum) + TAB
						+ df.format(sum == 0 ? 1 : other * 1.0 / sum);

				// active duration
				String play_gap = StatFunc.getTwoDay(play_last_seen, play_first_seen);
				String scan_gap = StatFunc.getTwoDay(scan_last_seen, scan_first_seen);

				// scan refer_domain
				int cnt_baidu = referMap.containsKey("baidu.com") ? referMap.get("baidu.com") : 0;
				int cnt_all_scan = 0;
				Iterator<Integer> it = referMap.values().iterator();
				while (it.hasNext()) {
					int val = (int) it.next();
					cnt_all_scan += val;
				}
				String refer_stat = StatFunc.getStat(referMap);
				String rate_refer_baidu = df.format(cnt_all_scan == 0 ? 1 : (cnt_baidu * 1.0 / cnt_all_scan));

				Integer[] arr = new Integer[aList.size()];
				aList.toArray(arr);
				String url_stat = MathStatics.CombineStat(arr);

				// Identity information
				String user_type = "";
				user_type = typeSet.toString().replace(" ", "");
				user_type = user_type.substring(1, user_type.length() - 1);
				int vip_type = StatFunc.getVipType(user_type);
				int shoumai_type = StatFunc.getShoumaiType(user_type);
				
				// vip_type + shoumai_type
				// rate_type
				// play_time + TAB + gt1min + TAB + lt1min + TAB + play_gap
				// style_stat + TAB + lang_stat + TAB + rate_stat + TAB +
				// singer_stat
				// publishtime_stat + TAB + category + TAB + film
				// play_work_holiy + TAB + scan_work_holiy
				// refer_stat + TAB + rate_refer_baidu + TAB + url_stat
				// favor_cnt
				/*new Text(rate_type + TAB + song_stat + TAB + play_time + TAB + gt1min + TAB + lt1min + TAB
				+ play_gap + TAB + style_stat + TAB + lang_stat + TAB + rate_stat + TAB + singer_stat + TAB
				+ publishtime_stat + TAB + category + TAB + film + TAB + play_hourSB.toString() + TAB
				+ play_daySB.toString() + TAB + play_weekSB.toString() + TAB + scan_hourSB.toString() + TAB
				+ scan_daySB.toString() + TAB + scan_weekSB.toString() + TAB + play_work_holiy + TAB
				+ scan_work_holiy + TAB + refer_stat + TAB + rate_refer_baidu + TAB + url_stat + TAB
				+ scan_gap + TAB + favor_cnt));*/
				System.out.println(baiduid + TAB + vip_type + TAB + shoumai_type + TAB + 
								"rate_type" + rate_type + TAB +
								"song_stat" + song_stat + TAB +
								"play_time" + play_time + TAB +
								"gt1min" + gt1min + TAB +
								"lt1min" + lt1min + TAB +
								"play_gap" + play_gap + TAB +
								"style_stat" + style_stat + TAB +
								"lang_stat" + lang_stat + TAB +
								"rate_stat" + rate_stat + TAB +
								"singer_stat" + singer_stat + TAB +
								"publishtime_stat" + publishtime_stat + TAB +
								"category" + category + TAB +
								"film" + film + TAB +
								"play_hourSB" + play_hourSB.toString() + TAB +
								"play_daySB" + play_daySB.toString() + TAB +
								"play_weekSB" + play_weekSB.toString() + TAB +
								"scan_hourSB" + scan_hourSB.toString() + TAB +
								"scan_daySB" + scan_daySB.toString() + TAB +
								"scan_weekSB" + scan_weekSB.toString() + TAB +
								"play_work_holiy" + play_work_holiy + TAB +
								"scan_work_holiy" + scan_work_holiy + TAB +
								"refer_stat" + refer_stat + TAB +
								"rate_refer_baidu" + rate_refer_baidu + TAB +
								"url_stat" + url_stat + TAB +
								"scan_gap" + scan_gap + TAB +
								"favor_cnt" + favor_cnt
								);
				System.out.println(songMap.toString());
				read.close();
			} else {
				System.out.println("找不到指定的文件");
			}
		} catch (Exception e) {
			System.out.println("读取文件内容出错");
			e.printStackTrace();
		}
	}

	public static HashMap<String, Integer> AddMap(HashMap<String, Integer> map, String key) {
		if (map.containsKey(key)) {
			int cnt = map.get(key);
			map.put(key, cnt + 1);
		} else {
			map.put(key, 1);
		}
		return map;
	}

	public static void main(String argv[]) {
		String filePath = "C:\\Users\\Administrator\\Desktop\\to_test.txt";
		readTxtFile(filePath);
	}
}
