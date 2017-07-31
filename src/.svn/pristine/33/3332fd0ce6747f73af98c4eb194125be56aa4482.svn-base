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
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.Map.Entry;

public class StatSongsDist {
	private final static String TAB = "\t";
	private static class UserMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] lparts = line.split(TAB);
			if (11 != lparts.length) return;
			String songid = lparts[1];
			String play_info = lparts[2];
			String order_info = lparts[3];
			String style = lparts[5].length() > 0 ? lparts[5] : "unknown";
			String language = lparts[6].length() > 0 ? lparts[6] : "unknown";
			String publish_time = lparts[7].length() > 0 ? lparts[7] : "0000-00-00";
			context.write(new Text(songid + TAB + style + TAB + language + TAB + publish_time), 
					new Text(play_info + TAB + order_info));
		}
	}

	private static class UserReduce extends Reducer<Text, Text, Text, Text> {
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
			// songid {play_list}
			HashMap<String, Integer> playtimeMap = new HashMap<String, Integer>();
			HashMap<String, Integer> rateMap = new HashMap<String, Integer>();
			TreeSet<String> typeSet = new TreeSet<String>();
			long count = 0;
			int dist_count = 0;
			try {
				for (Text val : values) {
					dist_count += 1;
					String[] vparts = val.toString().split(TAB);
					if (2 != vparts.length) continue;
					String order_info = vparts[1];
					String[] ods_part = order_info.split(";");
					if (ods_part.length > 100) continue;
					for (String ods : ods_part) {
						typeSet.add(ods.split("\\|")[0]);
						if (typeSet.size() > 1000) break;
					}
					String play_json = vparts[0];
					JSONObject play = JSON.parseObject(play_json);
					for (Entry<String, Object> entry : play.entrySet()) {
						count += 1;
						String getValue = entry.getValue().toString();
						String[] arr = getValue.split(TAB);
						if (10 != arr.length)
							continue;
						// logDate\singer_id\p100ms\p60s\pend\time\favor\ref\rate\event_userid
						String p100ms = arr[2];
						String p60s = arr[3];
						String ptime = p100ms.equals("1") ? "p100ms" : (p60s.equals("1") ? "p60s" : "pend");
						playtimeMap = AddMap(playtimeMap, ptime);
						String rate = arr[8];
						rateMap = AddMap(rateMap, rate);
					}
				}
			} catch (JSONException e) {
				// TODO nothing..
			} catch (ClassCastException e) {
				// TODO nothing..
			}
			String type = typeSet.toString().replace(" ", "");
			type = type.substring(1, type.length()-1);
			String play_time = getPlaytime(playtimeMap);
			String rate = rateMap.toString().replace(" ", "");
			rate = rate.substring(1, rate.length()-1);
			context.write(key, new Text(dist_count + TAB + count + TAB + play_time + TAB + rate + TAB + type));
		}
	}

	private static class CountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		Text kText = new Text();
		Text akText = new Text();
		LongWritable vInt = new LongWritable(1);
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 12444652 pop english 0000-00-00 1 2 1 0 1 000=1,128=8 no_order
			String[] lparts = value.toString().split(TAB);
			if (11 == lparts.length) {
				int cnt = Integer.valueOf(lparts[4]);
				String type = lparts[10];
				if (cnt < 5) {
					akText.set("all\t[0,5)");
					kText.set(type + "\t[0,5)");
				} else if (cnt < 10) {
					akText.set("all\t[5,10)");
					kText.set(type + "\t[5,10)");
				} else if (cnt < 50) {
					akText.set("all\t[10,50)");
					kText.set(type + "\t[10,50)");
				} else if (cnt < 100) {
					akText.set("all\t[50,100)");
					kText.set(type + "\t[50,100)");
				} else if (cnt < 500) {
					akText.set("all\t[100,500)");
					kText.set(type + "\t[100,500)");
				} else if (cnt < 1000) {
					akText.set("all\t[500,1000)");
					kText.set(type + "\t[500,1000)");
				} else if (cnt < 5000) {
					akText.set("all\t[1000,5000)");
					kText.set(type + "\t[1000,5000)");
				} else if (cnt < 10000) {
					akText.set("all\t[5000,10000)");
					kText.set(type + "\t[5000,10000)");
				} else {
					akText.set("all\t[10000,Inf)");
					kText.set(type + "\t[10000,Inf)");
				}
				context.write(akText, vInt);
				context.write(kText, vInt);
			}
		}
	}

	private static class CountReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
		protected void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long count = 0;
			for (LongWritable val : values) {
				count += val.get();
			}
			context.write(key, new LongWritable(count));
		}
	}

	private static class StyleMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 12444652 pop english 0000-00-00 1 2 1 0 1
			String[] lparts = value.toString().split(TAB);
			if (11 == lparts.length) {
				String style = lparts[1];
				String language = lparts[2];
				String type = lparts[10];
				context.write(new Text("all"), new Text(lparts[4] + TAB + lparts[5]));
				context.write(new Text("all" + TAB + "STYLE#" + style), new Text(lparts[4] + TAB + lparts[5]));
				context.write(new Text(type + TAB + "STYLE#" + style), new Text(lparts[4] + TAB + lparts[5]));
				context.write(new Text("all" + TAB + "LANGUAGE#" + language), new Text(lparts[4] + TAB + lparts[5]));
				context.write(new Text(type + TAB + "LANGUAGE#" + language), new Text(lparts[4] + TAB + lparts[5]));
			}
		}
	}

	private static class StyleReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			long dist = 0;
			long count = 0;
			for (Text val : values) {
				String[] vparts = val.toString().split(TAB);
				if (2 == vparts.length) {
					dist += Integer.valueOf(vparts[0]);
					count += Integer.valueOf(vparts[1]);
				}
			}
			context.write(key, new Text(dist + TAB + count));
		}
	}

	private static class PublishtimeMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 12444652 pop english 2015-10-19 1 2 1 0 1
			String[] lparts = value.toString().split(TAB);
			if (11 == lparts.length) {
				String time = lparts[3].split("-")[0];
				String type = lparts[10];
				context.write(new Text("all\t" + time), new Text(lparts[4] + TAB + lparts[5]));
				context.write(new Text(type + TAB + time), new Text(lparts[4] + TAB + lparts[5]));
			}
		}
	}

	private static class PublishtimeReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			long dist = 0;
			long count = 0;
			for (Text val : values) {
				String[] vparts = val.toString().split(TAB);
				if (2 == vparts.length) {
					dist += Integer.valueOf(vparts[0]);
					count += Integer.valueOf(vparts[1]);
				}
			}
			context.write(key, new Text(dist + TAB + count));
		}
	}

	private static class RateMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 12444652 pop english 2015-10-19 1 2 1 0 1 {000=1,128=5}
			String[] lparts = value.toString().split(TAB);
			if (11 == lparts.length) {
				String rate = lparts[9];
				String type = lparts[10];
				String[] rpart = rate.split(",");
				for (String r : rpart) {
					String[] kv = r.split("=");
					if (2 != kv.length) continue;
					context.write(new Text("all\t" + kv[0]), new LongWritable(Long.valueOf(kv[1])));
					context.write(new Text(type + TAB + kv[0]), new LongWritable(Long.valueOf(kv[1])));
				}
			}
		}
	}

	private static class RateReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
		protected void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long count = 0;
			for (LongWritable val : values) {
				count += val.get();
			}
			context.write(key, new LongWritable(count));
		}
	}

	public static boolean runLoadMapReducue(Configuration conf, String input)
			throws IOException, ClassNotFoundException, InterruptedException {
		String output1 = "/user/work/evan/tmp/StatSongsDist1";
		String output2 = "/user/work/evan/tmp/StatSongsDist2";
		String output3 = "/user/work/evan/tmp/StatSongsDist3";
		String output4 = "/user/work/evan/tmp/StatSongsDist4";
		String output5 = "/user/work/evan/tmp/StatSongsDist5";
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(new Path(output1), true);
		hdfs.delete(new Path(output2), true);
		hdfs.delete(new Path(output3), true);
		hdfs.delete(new Path(output4), true);
		hdfs.delete(new Path(output5), true);

		Job job = Job.getInstance(conf);
		job.setJarByClass(StatSongsDist.class);
		job.setJobName("Evan_StatSongsDist-L1");
		job.setNumReduceTasks(500);
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
		job.setJarByClass(StatSongsDist.class);
		job.setJobName("Evan_StatSongsDist-L2");
		job.setNumReduceTasks(1);
		job.setMapperClass(CountMapper.class);
		job.setCombinerClass(CountReduce.class);
		job.setReducerClass(CountReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.setInputPaths(job, output1);
		FileOutputFormat.setOutputPath(job, new Path(output2));
		job.waitForCompletion(true);

		job = Job.getInstance(conf);
		job.setJarByClass(StatSongsDist.class);
		job.setJobName("Evan_StatSongsDist-L3");
		job.setNumReduceTasks(1);
		job.setMapperClass(StyleMapper.class);
		job.setCombinerClass(StyleReduce.class);
		job.setReducerClass(StyleReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, output1);
		FileOutputFormat.setOutputPath(job, new Path(output3));
		job.waitForCompletion(true);

		job = Job.getInstance(conf);
		job.setJarByClass(StatSongsDist.class);
		job.setJobName("Evan_StatSongsDist-L4");
		job.setNumReduceTasks(1);
		job.setMapperClass(PublishtimeMapper.class);
		job.setCombinerClass(PublishtimeReduce.class);
		job.setReducerClass(PublishtimeReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, output1);
		FileOutputFormat.setOutputPath(job, new Path(output4));
		job.waitForCompletion(true);
		
		job = Job.getInstance(conf);
		job.setJarByClass(StatSongsDist.class);
		job.setJobName("Evan_StatSongsDist-L5");
		job.setNumReduceTasks(1);
		job.setMapperClass(RateMapper.class);
		job.setCombinerClass(RateReduce.class);
		job.setReducerClass(RateReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.setInputPaths(job, output1);
		FileOutputFormat.setOutputPath(job, new Path(output5));
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

		StatSongsDist.runLoadMapReducue(conf, args[0]);
	}
}