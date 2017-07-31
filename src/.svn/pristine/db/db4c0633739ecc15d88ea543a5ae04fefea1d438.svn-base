package com.baidumusic.useranaly;

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
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.baidumusic.vipuser.Tools;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeSet;

public class StatActiveTimeDist {
	private final static String TAB = "\t";
	private static final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private static class UserMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] lparts = line.split(TAB);
			if (11 != lparts.length) return;
			context.write(new Text(lparts[0]), new Text(lparts[2] + TAB + lparts[3]));
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
		
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// note: baiduid song_id {logDate\singer_id\p100ms\p60s\pend\time\favor\ref\rate\event_userid} order_info
			// note: singer_id\style\language\publish_time\category\si_has_filmtv
			// note: date-hour\device_type\product\refer_domain
			HashMap<String, Integer> weekMap = new HashMap<String, Integer>();
			HashMap<String, Integer> dayMap = new HashMap<String, Integer>();
			HashMap<String, Integer> hourMap = new HashMap<String, Integer>();
			HashMap<String, Integer> holidayMap = new HashMap<String, Integer>();
			HashMap<String, Integer> holiurMap = new HashMap<String, Integer>();

			TreeSet<String> typeSet = new TreeSet<String>();
			for (Text val : values) {
				String[] vparts = val.toString().split(TAB);
				if (2 != vparts.length) continue;
				String order_info = vparts[1];
				String[] ods_part = order_info.split(";");
				if (ods_part.length > 100) continue;
				for (String ods : ods_part) {
					typeSet.add(ods.split("\\|")[0]);
					if (typeSet.size() > 1000) break;
				}
				try {
					String play_json = vparts[0];
					JSONObject play = JSON.parseObject(play_json);
					for (Entry<String, Object> entry : play.entrySet()) {
						String date = Tools.formatDate2(entry.getKey(), true);
						Calendar cal = Calendar.getInstance();
						cal.setTime(df.parse(date));
						int week = cal.get(Calendar.DAY_OF_WEEK) - 1;
						int day = cal.get(Calendar.DAY_OF_MONTH);
						int hour = cal.get(Calendar.HOUR_OF_DAY);
						String holiday = "Workday";
						if (CheckHoliday.checkHoliday(date)) {
							holiday = "Holiday";
						}

						weekMap = AddMap(weekMap, String.valueOf(week));
						dayMap = AddMap(dayMap, String.valueOf(day));
						hourMap = AddMap(hourMap, String.valueOf(hour));
						holidayMap = AddMap(holidayMap, holiday);
						holiurMap = AddMap(holiurMap, new String(holiday + "_" + hour));
					}
				} catch (JSONException e) {
					// TODO nothing..
				} catch (ClassCastException e) {
					// TODO nothing..
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			String type = typeSet.toString().replace(" ", "");
			type = type.substring(1, type.length()-1);
			for (Entry<String, Integer> entry : weekMap.entrySet()) {
				String k = entry.getKey();
				int v = entry.getValue();
				context.write(new Text("all"), new Text("week_" + k + TAB + v));
				context.write(new Text(type), new Text("week_" + k + TAB + v));
			}

			for (Entry<String, Integer> entry : dayMap.entrySet()) {
				String k = entry.getKey();
				int v = entry.getValue();
				context.write(new Text("all"), new Text("day_" + k + TAB + v));
				context.write(new Text(type), new Text("day_" + k + TAB + v));
			}

			for (Entry<String, Integer> entry : hourMap.entrySet()) {
				String k = entry.getKey();
				int v = entry.getValue();
				context.write(new Text("all"), new Text("hour_" + k + TAB + v));
				context.write(new Text(type), new Text("hour_" + k + TAB + v));
			}

			for (Entry<String, Integer> entry : holidayMap.entrySet()) {
				String k = entry.getKey();
				int v = entry.getValue();
				context.write(new Text("all"), new Text(k + TAB + v));
				context.write(new Text(type), new Text(k + TAB + v));
			}

			for (Entry<String, Integer> entry : holiurMap.entrySet()) {
				String k = entry.getKey();
				int v = entry.getValue();
				context.write(new Text("all"), new Text(k + TAB + v));
				context.write(new Text(type), new Text(k + TAB + v));
			}
			
			/*context.write(key, new Text(typeSet.toString().replace(" ", "")
					+ TAB + weekMap.toString().replace(" ", "")
					+ TAB + dayMap.toString().replace(" ", "")
					+ TAB + hourMap.toString().replace(" ", "")
					+ TAB + holidayMap.toString().replace(" ", "")
					));*/
		}
	}
	
	private static class TimeMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//king,kingbao,lossless,no_order,shoumai,vip_order	5	19
			String line = value.toString();
			String[] lpart = line.split(TAB);
			if (3 == lpart.length) {
				context.write(new Text(lpart[0] + TAB + lpart[1]), 
						new Text("1" + TAB + lpart[2]));
			} 
		}
	}
	
	private static class TimeReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			long count = 0;
			int dist = 0;
			for (Text val : values) {
				String[] vpart = val.toString().split(TAB);
				if (2 != vpart.length) continue;
				dist += Integer.valueOf(vpart[0]);
				count += Long.valueOf(vpart[1]);
			}
			context.write(key, new Text(dist + TAB + count));
		}
	}

	public static boolean runLoadMapReducue(Configuration conf, String input)
			throws IOException, ClassNotFoundException, InterruptedException {
		String output1 = "/user/work/evan/tmp/StatActiveTimeDist1";
		String output2 = "/user/work/evan/tmp/StatActiveTimeDist2";
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(new Path(output1), true);
		hdfs.delete(new Path(output2), true);
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(StatActiveTimeDist.class);
		job.setJobName("Evan_StatActiveTimeDist-L1");
		job.setNumReduceTasks(1000);
		job.setMapperClass(UserMapper.class);
		job.setReducerClass(UserReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, new Path(output1));
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		job.waitForCompletion(true);

		job = Job.getInstance(conf);
		job.setJarByClass(StatActiveTimeDist.class);
		job.setJobName("Evan_StatActiveTimeDisto-L2");
		job.setNumReduceTasks(1);
		job.setMapperClass(TimeMapper.class);
		job.setCombinerClass(TimeReduce.class);
		job.setReducerClass(TimeReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, output1);
		FileOutputFormat.setOutputPath(job, new Path(output2));
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

		StatActiveTimeDist.runLoadMapReducue(conf, args[0]);
	}
}