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
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.Map.Entry;

public class StatUserSongDist {
	private final static String TAB = "\t";

	private static class GetTopMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] lparts = line.split(TAB);
			if (11 != lparts.length)
				return;
			String baiduid = lparts[0];
			String order_info = lparts[3];
			context.write(new Text(baiduid), new Text("0" + TAB + order_info));
		}
	}

	private static class GetTopCombiner extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			TreeSet<String> typeSet = new TreeSet<String>();
			int dist_count = 0;
			for (Text val : values) {
				dist_count += 1;
				String order_info = val.toString();
				String[] ods_part = order_info.split(";");
				if (ods_part.length > 100)
					continue;
				for (String ods : ods_part) {
					typeSet.add(ods.split("\\|")[0]);
					if (typeSet.size() > 1000)
						break;
				}
			}
			for (String type : typeSet) {
				context.write(key, new Text(dist_count + TAB + type));
			}
		}
	}

	private static class GetTopReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			TreeSet<String> typeSet = new TreeSet<String>();
			int dist_count = 0;
			for (Text val : values) {
				String[] vparts = val.toString().split(TAB);
				if (2 != vparts.length)
					continue;
				dist_count += Integer.valueOf(vparts[0]);
				String order_info = vparts[1];
				String[] ods_part = order_info.split(";");
				if (ods_part.length > 100)
					continue;
				for (String ods : ods_part) {
					typeSet.add(ods.split("\\|")[0]);
					if (typeSet.size() > 1000)
						break;
				}
			}
			if (dist_count > 1000) {
				String type = typeSet.toString().replace(" ", "");
				type = type.substring(1, type.length() - 1);
				context.write(key, new Text(type));
			}
		}
	}

	private static class UserMapper extends Mapper<LongWritable, Text, Text, Text> {
		private static HashMap<String, String> hMap = new HashMap<String, String>();
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			File popsong = new File("top.type");
			BufferedReader br = new BufferedReader(new FileReader(popsong));
			String line = "";
			while ((line = br.readLine()) != null) {
				String[] lparts = line.split(TAB);
				if (2 != lparts.length)
					continue;
				hMap.put(lparts[0], lparts[1]);
			}
			br.close();
			System.out.println("hMap.size() = " + hMap.size());
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
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] lparts = line.split(TAB);
			if (11 != lparts.length)
				return;
			String baiduid = lparts[0];
			String play_info = lparts[2];
			String order_info = lparts[3];
			String style = lparts[5].length() > 0 ? lparts[5] : "unknown";
			String language = lparts[6].length() > 0 ? lparts[6] : "unknown";
			String publish_time = lparts[7].length() > 0 ? lparts[7] : "0000-00-00";
			int times = 0;
			HashMap<String, Integer> rateMap = new HashMap<String, Integer>();
			try {
				JSONObject play = JSON.parseObject(play_info);
				times = play.size();
				for (Entry<String, Object> entry : play.entrySet()) {
					String getValue = entry.getValue().toString();
					String[] arr = getValue.split(TAB);
					if (10 != arr.length) continue;
					String rate = arr[8];
					rateMap = AddMap(rateMap, rate);
				}
			} catch (JSONException e) {
				// TODO nothing..
			} catch (ClassCastException e) {
				// TODO nothing..
			}
			String rate = rateMap.toString().replace(" ", "");
			rate = rate.substring(1, rate.length()-1);
			if (hMap.containsKey(baiduid)) {
				String type = hMap.get(baiduid);
				context.write(new Text("DONE"), new Text(
						type + "###" + style + TAB + language + TAB + publish_time + TAB + times + TAB + rate));
			} else {
				context.write(new Text(baiduid), new Text(
						style + TAB + language + TAB + publish_time + TAB + times + TAB + rate + "###" + order_info));
			}
		}
	}

	private static class UserReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String k = key.toString();
			if (k.equals("DONE")) {
				for (Text val : values) {
					String[] vparts = val.toString().split("###");
					if (2 == vparts.length) {
						context.write(new Text(vparts[0]), new Text(vparts[1]));
					}
				}
				return;
			}
			TreeSet<String> typeSet = new TreeSet<String>();
			HashSet<String> hSet = new HashSet<String>();
			for (Text val : values) {
				String[] vparts = val.toString().split("###");
				if (2 != vparts.length) continue;
				hSet.add(vparts[0]);
				String order_info = vparts[1];
				String[] ods_part = order_info.split(";");
				if (ods_part.length > 100) continue;
				for (String ods : ods_part) {
					typeSet.add(ods.split("\\|")[0]);
					if (typeSet.size() > 1000)
						break;
				}
			}
			if (typeSet.size() < 1) return;
			String type = typeSet.toString().replace(" ", "");
			type = type.substring(1, type.length() - 1);
			for (String info : hSet) {
				context.write(new Text(type), new Text(info));
			}
		}
	}
	
	private static class StyleMapper extends Mapper<LongWritable, Text, Text, Text> {
		String one = new String("1");
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//new Text(type), new Text(style + TAB + language + TAB + publish_time + TAB + times)
			String[] lparts = value.toString().split(TAB);
			if (6 == lparts.length) {
				String type = lparts[0];
				String style = lparts[1];
				String language = lparts[2];
				String times = lparts[4];
				context.write(new Text("all"), new Text(one + TAB + times));
				context.write(new Text("all" + TAB + "STYLE#" + style), new Text(one + TAB + times));
				context.write(new Text(type + TAB + "STYLE#" + style), new Text(one + TAB + times));
				context.write(new Text("all" + TAB + "LANGUAGE#" + language), new Text(one + TAB + times));
				context.write(new Text(type + TAB + "LANGUAGE#" + language), new Text(one + TAB + times));
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
		String one = new String("1");
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//new Text(type), new Text(style + TAB + language + TAB + publish_time + TAB + times)
			String[] lparts = value.toString().split(TAB);
			if (6 == lparts.length) {
				String time = lparts[3].split("-")[0];
				String type = lparts[0];
				String times = lparts[4];
				context.write(new Text("all\t" + time), new Text(one + TAB + times));
				context.write(new Text(type + TAB + time), new Text(one + TAB + times));
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
			String[] lparts = value.toString().split(TAB);
			if (6 == lparts.length) {
				String rate = lparts[5];
				String type = lparts[0];
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
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		String output1 = "/user/work/evan/tmp/StatUserSongDist1";
		String output2 = "/user/work/evan/tmp/StatUserSongDist2";
		String output3 = "/user/work/evan/tmp/StatUserSongDist3";
		String output4 = "/user/work/evan/tmp/StatUserSongDist4";
		String output5 = "/user/work/evan/tmp/StatUserSongDist5";
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(new Path(output1), true);
		hdfs.delete(new Path(output2), true);
		hdfs.delete(new Path(output3), true);
		hdfs.delete(new Path(output4), true);
		hdfs.delete(new Path(output5), true);

		Job job = Job.getInstance(conf);
		job.setJarByClass(StatUserSongDist.class);
		job.setJobName("Evan_StatUserSongDist-L1");
		job.setNumReduceTasks(1);
		job.setMapperClass(GetTopMapper.class);
		job.setCombinerClass(GetTopCombiner.class);
		job.setReducerClass(GetTopReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, new Path(output1));
		job.waitForCompletion(true);

		job = Job.getInstance(conf);
		job.setJarByClass(StatUserSongDist.class);
		job.setJobName("Evan_StatUserSongDist-L2");
		job.setNumReduceTasks(1000);
		job.setMapperClass(UserMapper.class);
		job.setReducerClass(UserReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.addCacheFile(new URI(output1 + "/part-r-00000#top.type"));
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, new Path(output2));
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		job.waitForCompletion(true);

		job = Job.getInstance(conf);
		job.setJarByClass(StatUserSongDist.class);
		job.setJobName("Evan_StatUserSongDist-L3");
		job.setNumReduceTasks(1);
		job.setMapperClass(StyleMapper.class);
		job.setCombinerClass(StyleReduce.class);
		job.setReducerClass(StyleReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, output2);
		FileOutputFormat.setOutputPath(job, new Path(output3));
		job.waitForCompletion(true);

		job = Job.getInstance(conf);
		job.setJarByClass(StatUserSongDist.class);
		job.setJobName("Evan_StatUserSongDist-L4");
		job.setNumReduceTasks(1);
		job.setMapperClass(PublishtimeMapper.class);
		job.setCombinerClass(PublishtimeReduce.class);
		job.setReducerClass(PublishtimeReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, output2);
		FileOutputFormat.setOutputPath(job, new Path(output4));
		job.waitForCompletion(true);
		
		job = Job.getInstance(conf);
		job.setJarByClass(StatUserSongDist.class);
		job.setJobName("Evan_StatUserSongDist-L5");
		job.setNumReduceTasks(1);
		job.setMapperClass(RateMapper.class);
		job.setCombinerClass(RateReduce.class);
		job.setReducerClass(RateReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.setInputPaths(job, output2);
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

		StatUserSongDist.runLoadMapReducue(conf, args[0]);
	}
}