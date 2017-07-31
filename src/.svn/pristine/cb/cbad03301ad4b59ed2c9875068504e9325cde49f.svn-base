package com.baidumusic.useranaly;

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
import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

public class StatFansOfSinger {
	private final static String TAB = "\t";
	private static final HashSet<String> singerSet = new HashSet<String>();
	static {
		singerSet.add("198022987");
		singerSet.add("211119691");
		singerSet.add("1121");
		singerSet.add("1071");
		singerSet.add("1030");
		singerSet.add("5564");
		singerSet.add("7994");
		singerSet.add("5890");
		singerSet.add("2397");
		singerSet.add("6113");
		singerSet.add("1246");
	}
	
	private static HashMap<String, Integer> AddMap(HashMap<String, Integer> map, String key) {
		if (map.containsKey(key)) {
			int cnt = map.get(key);
			map.put(key, cnt + 1);
		} else {
			map.put(key, 1);
		}
		return map;
	}
	
	private static class UserMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] lparts = line.split(TAB);
			if (11 != lparts.length) return;
			
			String baiduid = lparts[0];
			String play_json = lparts[2];
			JSONObject play = JSON.parseObject(play_json);
			HashMap<String, Integer> hMap = new HashMap<String, Integer>();
			for (Entry<String, Object> entry : play.entrySet()) {
				String getValue = entry.getValue().toString();
				String[] arr = getValue.split(TAB);
				if (10 != arr.length) continue;
				//{logDate\singer_id\p100ms\p60s\pend\time\favor\ref\rate\event_userid}
				String singer_id = arr[1];
				String p60s = arr[3];
				if (p60s.equals("1")) {
					for (String singer : singerSet) {
						if (singer_id.contains(singer)) {
							if (singer.equals("198022987")) {
								singer = "211119691";
							}
							hMap = AddMap(hMap, singer);
						}
					}
				}
			}
			String style = lparts[5].length() == 0 ? "unknown" : lparts[5];
			String language = lparts[6].length() == 0 ? "unknown" : lparts[6];
			if (hMap.size() > 0) {
				for (Entry<String, Integer> entry : hMap.entrySet()) {
					String singer = entry.getKey();
					int cnt = entry.getValue();
					context.write(new Text(singer + TAB + baiduid), 
							new Text(cnt + TAB + language.replace(",", "|") + TAB + style.replace(",", "|")));
				}
			}
		}
	}

	private static class UserReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Integer> langMap = new HashMap<String, Integer>();
			HashMap<String, Integer> styleMap = new HashMap<String, Integer>();
			int count = 0;
			for (Text val : values) {
				String[] vpart = val.toString().split(TAB);
				String cnt = vpart[0];
				count += Integer.valueOf(cnt);

				String language = vpart[1];
				langMap = AddMap(langMap, language.length() == 0 ? "unknown" : language);

				String style = vpart[2];
				styleMap = AddMap(styleMap, style.length() == 0 ? "unknown" : style);
			}
			if (count >= 10) {
				context.write(key, new Text(count + TAB + langMap.toString().replace(" ", "") 
						+ TAB + styleMap.toString().replace(" ", "")));
			}
		}
	}
	
	public static boolean runLoadMapReducue(Configuration conf, String input)
			throws IOException, ClassNotFoundException, InterruptedException {
		String output = "/user/work/evan/tmp/StatFansOfSinger";
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(new Path(output), true);
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(StatFansOfSinger.class);
		job.setJobName("Evan_StatFansOfSinger");
		job.setNumReduceTasks(1);
		job.setMapperClass(UserMapper.class);
		job.setReducerClass(UserReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, new Path(output));
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

		StatFansOfSinger.runLoadMapReducue(conf, args[0]);
	}
}