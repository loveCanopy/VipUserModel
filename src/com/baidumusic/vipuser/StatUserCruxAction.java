package com.baidumusic.vipuser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StatUserCruxAction {
	private final static String TAB = "\t";
	private static class UserMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			int idx = line.indexOf(TAB);
			if (idx > 0) {
				context.write(new Text(line.substring(0, idx)), new Text(line.substring(idx + 1)));
			}
		}
	}

	private static class UserReduce extends Reducer<Text, Text, Text, Text> {
		private HashMap<String, Integer> AddMap(HashMap<String, Integer> map, String key) {
			if (map.containsKey(key)) {
				int cnt = map.get(key);
				map.put(key, cnt + 1);
			} else {
				map.put(key, 1);
			}
			return map;
		}

		private String getStat(HashMap<String, Integer> hMap) {
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

		private int getSingleRate(HashMap<String, Integer> hMap, String size) {
			if (hMap.containsKey(size)) {
				return hMap.get(size);
			} else {
				return 0;
			}
		}

		private String formatRate(String rate) {
			String res = "000";
			if (!rate.matches("[0-9]+")) {
				return res;
			}
			int irate = Integer.valueOf(rate);
			if (0 == irate) {
				return res;
			}
			if (irate < 64 || irate < (64+96)/2) {
				res = "64";
			} else if (irate < 96 || irate < (96+128)/2) {
				res = "96";
			} else if (irate < 128 || irate < (128+192)/2) {
				res = "128";
			} else if (irate < 192 || irate < (192+256)/2) {
				res = "192";
			} else if (irate < 256 || irate < (256+320)/2) {
				res = "256";
			} else if (irate < 320 || irate < (320+500)/2) {
				res = "320";
			} else {
				res = "other";
			}
			
			return res;
		}
		
		private String getRate(HashMap<String, Integer> hMap) {
			String rate = "0\t0\t0\t0\t0\t0\t0\t0\t0";
			if (hMap.size() == 0) {
				return rate;
			}
			int r_000 = getSingleRate(hMap, "000");
			int r_64 = getSingleRate(hMap, "64");
			int r_96 = getSingleRate(hMap, "96");
			int r_128 = getSingleRate(hMap, "128");
			int r_192 = getSingleRate(hMap, "192");
			int r_256 = getSingleRate(hMap, "256");
			int r_320 = getSingleRate(hMap, "320");
			int r_500 = getSingleRate(hMap, "500");
			int r_other = getSingleRate(hMap, "other");
			return r_000 + TAB + r_64 + TAB + r_96 + TAB + r_128 + TAB + 
					r_192 + TAB + r_256 + TAB + r_320 + TAB + r_500 + TAB + r_other;
		}

		private String getPlaytime(HashMap<String, Integer> hMap) {
			String time = "0\t0\t0";
			if (hMap.size() == 0) {
				return time;
			}
			int p100ms = hMap.containsKey("p100ms") ? hMap.get("p100ms") : 0;
			int p60s = hMap.containsKey("p60s") ? hMap.get("p60s") : 0;
			int pend = hMap.containsKey("pend") ? hMap.get("pend") : 0;
			return p100ms + TAB + p60s + TAB + pend;
		}

		private int getDay(String date) {
			String pattern = "^\\d{4}-\\d{2}-(\\d{2}) (\\d{2}):\\d{2}:\\d{2}$";
			Pattern p = Pattern.compile(pattern);
			Matcher matcher = p.matcher(date);
			if (matcher.matches()) {
				return Integer.valueOf(matcher.group(1));
			}

			return 0;
		}

		private int getHour(String date) { //2016-10-13 00:00:00
			String pattern = "^\\d{4}-\\d{2}-(\\d{2}) (\\d{2}):\\d{2}:\\d{2}$";
			Pattern p = Pattern.compile(pattern);
			Matcher matcher = p.matcher(date);
			if (matcher.matches()) {
				return Integer.valueOf(matcher.group(2));
			}

			return 24;
		}

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// note: baiduid song_id {logDate\singer_id\p100ms\p60s\pend\time\favor\ref\rate\event_userid} order_info
			// note: singer_id\style\language\publish_time\category\si_has_filmtv
			// note: date-hour\device_type\product\refer_domain
			HashMap<String, Integer> songMap = new HashMap<String, Integer>();
			HashMap<String, Integer> typeMap = new HashMap<String, Integer>();
			HashMap<String, Integer> langMap = new HashMap<String, Integer>();
			HashMap<String, Integer> rateMap = new HashMap<String, Integer>();
			HashMap<String, Integer> singerMap = new HashMap<String, Integer>();
			HashMap<String, Integer> playtimeMap = new HashMap<String, Integer>();
			int[] hourArr = new int[25];
			int[] dayArr = new int[32];
			StringBuilder hourSB = new StringBuilder();
			StringBuilder daySB = new StringBuilder();
			int favor_cnt = 0;
			for (Text val : values) {
				String[] vparts = val.toString().split(TAB);
				if (10 != vparts.length) continue;
				String song_id = vparts[0];
				songMap = AddMap(songMap, song_id);
				String play_json = vparts[1];
				String order_info = vparts[2];
				HashSet<String> typeSet = new HashSet<String>();
				String[] ods_part = order_info.split(";");
				for (String ods : ods_part) {
					typeSet.add(ods.split("\\|")[0]);
				}
				String user_type = "";
				user_type = typeSet.toString().replace(" ", "");
				user_type = user_type.substring(1, user_type.length()-1);
				//if (order_info.equals("no_login")) {
				//	user_type = "no_login";
				//} else if (order_info.contains("no_order")) {
				//	user_type = "login";
				//} else if (order_info.contains("vip_order")) {
				//	user_type = "vip";
				//} else {
				//	user_type = "pay";
				//}
				typeMap = AddMap(typeMap, user_type);
				String language = vparts[5];
				langMap = AddMap(langMap, language.length() == 0 ? "unknown" : language);
				try {
					JSONObject play = JSON.parseObject(play_json);
					for (Entry<String, Object> entry : play.entrySet()) {
						String date = entry.getKey();
						//dateSet.add(date);
						int hour = getHour(date);
						hourArr[hour] = hourArr[hour] + 1;

						int day = getDay(date);
						dayArr[day] = dayArr[day] + 1;
						String getValue = entry.getValue().toString();
						String[] arr = getValue.split(TAB);
						if (10 != arr.length) continue;
						String singer = arr[1];
						String p100ms = arr[2];
						String p60s = arr[3];
						String favor = arr[6];
						String rate = formatRate(arr[8]);

						String ptime = p100ms.equals("1") ? "p100ms" : (p60s.equals("1") ? "p60s" : "pend");
						playtimeMap = AddMap(playtimeMap, ptime);
						singerMap = AddMap(singerMap, singer);
						if (favor.equals("1")) {
							favor_cnt += 1;
						}
						rateMap = AddMap(rateMap, rate);
					}
				} catch (JSONException e) {
					// TODO nothing..
				} catch (ClassCastException e) {
					// TODO nothing..
				}
			}
			String song_stat = getStat(songMap);
			String lang_stat = getStat(langMap);
			String rate_stat = getRate(rateMap);
			String singer_stat = getStat(singerMap);
			String play_time = getPlaytime(playtimeMap);
			int cnt1 = 0;
			int cnt2 = 0;
			for (int h : hourArr) {
				if (cnt1++ == 0) {
					hourSB.append(h);
					continue;
				}
				hourSB.append(TAB).append(h);
			}
			for (int d : dayArr) {
				if (cnt2++ == 0) {
					daySB.append(d);
					continue;
				}
				daySB.append(TAB).append(d);
			}
			/*
			baiduid
			user_type (no_login/login/vip/pay)
			song_stat (sum/count/max/min/ave/median/std/mode)
			lang_stat (sum/count/max/min/ave/median/std/mode)
			rate_stat (000/64/96/128/192/256/320/500)
			singer_stat (sum/count/max/min/ave/median/std/mode)
			play_time (100ms/60s/end)
			day (1-32)
			hour (0-24)
			favor_cnt (count)
			 */
			context.write(new Text(key.toString() + TAB + typeMap.toString()),
					new Text(song_stat + TAB + lang_stat + TAB
							+ rate_stat + TAB + singer_stat + TAB + play_time + TAB
							+ daySB.toString() + TAB + hourSB.toString() + TAB + favor_cnt));
		}
	}

	public static boolean runLoadMapReducue(Configuration conf, String input, Path output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(StatUserCruxAction.class);
		job.setJobName("Evan_StatUserCruxAction");
		job.setNumReduceTasks(1000);
		job.setMapperClass(UserMapper.class);
		job.setReducerClass(UserReduce.class);
		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
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
		String out = "/user/work/evan/tmp/StatUserCruxAction";
		Path path = new Path(out);
		hdfs.delete(path, true);

		StatUserCruxAction.runLoadMapReducue(conf, args[0], new Path(out));
	}
}