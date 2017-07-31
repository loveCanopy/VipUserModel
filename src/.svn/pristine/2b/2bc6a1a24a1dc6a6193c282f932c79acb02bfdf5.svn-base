package com.baidumusic.vipuser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.baidumusic.useranaly.CheckHoliday;
import java.io.IOException;
import java.net.URISyntaxException;
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
import java.util.Map.Entry;
import java.util.TreeSet;

public class StatUserSongStyle {
	private final static String TAB = "\t";
	private static final DecimalFormat df = new DecimalFormat("0.000000");
	private static final DateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static final DateFormat dateformat2 = new SimpleDateFormat("yyyyMMdd-HH");

	private static class UserMapper extends Mapper<LongWritable, Text, Text, Text> {
		// protected void map(LongWritable key, Text value, Context context)
		// throws IOException, InterruptedException {
		// String line = value.toString();
		// int idx1 = line.indexOf(TAB);
		// if (idx1 > 0) {
		// int idx2 = line.substring(idx1 + 1).indexOf(TAB);
		// if (idx2 > 0) {
		// String baiduid = line.substring(idx1 + 1, idx1 + idx2 + 1);
		// String other = line.substring(idx1 + idx2 + 2);
		// context.write(new Text(baiduid), new Text(other));
		// }
		// }
		// }
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			int idx = line.indexOf(TAB);
			if (idx > 0) {
				String baiduid = line.substring(0, idx);
				String other = line.substring(idx + 1);
				context.write(new Text(baiduid), new Text(other));
			}
		}
	}

	private static class UserReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
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
			String scan = "null";
			
			for (Text val : values) {
				String[] vparts = val.toString().split(TAB);
				if (10 != vparts.length)
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
				String scan_json = vparts[9];

				if (!play_json.matches("\\{.*:.*\\}"))
					continue;
				
				// get scan
				if (scan_json.length() > scan.length()) {
					scan = scan_json;
				}

				// songs
				songMap = StatFunc.AddMap(songMap, song_id);

				// user type
				TreeSet<String> typeSet = new TreeSet<String>();
				String[] ods_part = order_info.split(";");
				for (String ods : ods_part) {
					typeSet.add(ods.split("\\|")[0]);
				}
				String user_type = "";
				user_type = typeSet.toString().replace(" ", "");
				user_type = user_type.substring(1, user_type.length() - 1);
				typeMap = StatFunc.AddMap(typeMap, user_type);

				// singers
				singerMap = StatFunc.AddMap(singerMap, singer_id);

				// style
				styleMap = StatFunc.AddMap(styleMap, style);

				// language
				langMap = StatFunc.AddMap(langMap, language.length() == 0 ? "unknown" : language);

				// publish_time
				String publish_year = StatFunc.formatPublishtime(publish_time.split("-")[0]);
				publishMap = StatFunc.AddMap(publishMap, publish_year);

				// category
				categoryMap = StatFunc.AddMap(categoryMap, category);

				// si_has_filmtv
				filmMap = StatFunc.AddMap(filmMap, si_has_filmtv);

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
						if (time.matches("[0-9]+") && Integer.valueOf(time) > 120000) {
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
			
			// user has no play records, exit.
			if (songMap.size() == 0) {
				return;
			}

			// Deal with scan
			if (scan.matches("\\{.*:.*\\}")) {
				JSONObject scan_json = JSON.parseObject(scan);
				for (Entry<String, Object> s : scan_json.entrySet()) {
					String gKey = s.getKey();
					String[] arr = gKey.split(TAB);
					String time = arr[0];
					try {
						String date = dateformat.format(dateformat2.parse(time));
						if (date.compareTo(scan_first_seen) < 0) {
							scan_first_seen = date;
						}
						if (date.compareTo(scan_last_seen) > 0) {
							scan_last_seen = date;
						}

						if (CheckHoliday.checkHoliday(date)) {
							scan_holiday += 1;
						} else {
							scan_workday += 1;
						}

						Calendar cal = Calendar.getInstance();
						cal.setTime(dateformat2.parse(time));
						int hour = cal.get(Calendar.HOUR_OF_DAY);
						int week = cal.get(Calendar.DAY_OF_WEEK) - 1;
						int day = cal.get(Calendar.DAY_OF_MONTH) - 1;
						scan_hourArr[hour] = scan_hourArr[hour] + 1;
						scan_dayArr[day] = scan_dayArr[day] + 1;
						scan_weekArr[week] = scan_weekArr[week] + 1;

					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					// String device_type = arr[1];
					// String product = arr[2];
					String refer_domain = arr[3];
					String domain = Tools.getTopPrivateDomain(refer_domain);
					if (domain.length() > 0) {
						referMap = StatFunc.AddMap(referMap, domain);
					}
					int gValue = Integer.valueOf(s.getValue().toString());
					aList.add(gValue);
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
			if (typeMap.size() == 0) {
				typeMap.put("no_login", 1);
			}
			
			for (Map.Entry<String, Integer> entry : typeMap.entrySet()) {
				String type = entry.getKey();
				typeSet.add(type);
			}
			
			/*int sum = no_login + no_order + other;
			String rate_type = df.format(sum == 0 ? 1 : no_login * 1.0 / sum) + TAB
					+ df.format(sum == 0 ? 1 : no_order * 1.0 / sum) + TAB
					+ df.format(sum == 0 ? 1 : other * 1.0 / sum);*/

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
			String baiduid = key.toString();
			
			// vip_type + shoumai_type
			// rate_type
			// song_stat
			// play_time + TAB + gt1min + TAB + lt1min + TAB + play_gap
			// style_stat + TAB + lang_stat + TAB + rate_stat + TAB +
			// singer_stat
			// publishtime_stat + TAB + category + TAB + film
			// play_work_holiy + TAB + scan_work_holiy
			// refer_stat + TAB + rate_refer_baidu + TAB + url_stat
			// favor_cnt
			/*
			baiduid
			vip_type + shoumai_type (no_login/no_login,no_order/no_order/no_order,vip_order/no_login,no_order,vip_order/vip_order/other);
			//rate_type (no_login_rate/no_order_rate/other_rate) 1-3
			song_stat (sum/count/max/min/ave/median/std/mode) 1-8
			play_time (p100ms_rate/p60s_rate/pend_rate/p100ms\p100ms/gt1min/lt1min/play_gap) 9-15
			style_stat (sum/count/max/min/ave/median/std/mode) 16-23
			lang_stat (sum/count/max/min/ave/median/std/mode) 24-31
			rate_stat (000/64/96/128/192/256/320/320plus) 32-39
			singer_stat (sum/count/max/min/ave/median/std/mode) 40-47
			publishtime_stat (1949/1960/1970/1980/1990/2000/2010/2017/other) 48-56
			category (putong_rate/erge_rate/duwu_rate) 57-59
			film (yes_rate/no_rate) 60-61
			play_hourSB (0-23) 62-85
			play_daySB (1-31) 86-116
			play_weekSB (1-7) 117-123
			scan_hourSB (0-23) 124-147
			scan_daySB (1-31) 148-178
			scan_weekSB (1-7) 179-185
			play_work_holiy/scan_work_holiy 186-191
			refer_stat (sum/count/max/min/ave/median/std/mode) 191-198
			rate_refer_baidu (double) 199
			url_stat (sum/count/max/min/ave/median/std/mode) 200-207
			scan_gap (double) 208
			favor_cnt (count) 209
			 */
			context.write(new Text(baiduid + TAB + vip_type + TAB + shoumai_type),
					new Text(song_stat + TAB + play_time + TAB + gt1min + TAB + lt1min + TAB
							+ play_gap + TAB + style_stat + TAB + lang_stat + TAB + rate_stat + TAB + singer_stat + TAB
							+ publishtime_stat + TAB + category + TAB + film + TAB + play_hourSB.toString() + TAB
							+ play_daySB.toString() + TAB + play_weekSB.toString() + TAB + scan_hourSB.toString() + TAB
							+ scan_daySB.toString() + TAB + scan_weekSB.toString() + TAB + play_work_holiy + TAB
							+ scan_work_holiy + TAB + refer_stat + TAB + rate_refer_baidu + TAB + url_stat + TAB
							+ scan_gap + TAB + favor_cnt));
		}
	}

	public static boolean runLoadMapReducue(Configuration conf, String input, Path output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(StatUserSongStyle.class);
		job.setJobName("Evan_StatUserSongStyle");
		job.setNumReduceTasks(1000);
		job.setMapperClass(UserMapper.class);
		job.setReducerClass(UserReduce.class);
		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		return job.waitForCompletion(true);
	}

	public static void main(String[] args)
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();

		if (args.length == 0) {
			System.err.println("Usage: rcfile <in>");
			System.exit(1);
		}

		String queue = "mapred";
		if (args.length > 1) {
			queue = args[1].matches("hql|dstream|mapred|udw|user|common") ? args[1] : "mapred";
		}
		conf.set("mapreduce.job.queuename", queue);

		FileSystem hdfs = FileSystem.get(conf);
		String out = "/user/work/evan/tmp/StatUserSongStyle";
		Path path = new Path(out);
		hdfs.delete(path, true);

		StatUserSongStyle.runLoadMapReducue(conf, args[0], new Path(out));
	}
}