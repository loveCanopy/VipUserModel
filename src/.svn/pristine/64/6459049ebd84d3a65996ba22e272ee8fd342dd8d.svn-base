package com.baidumusic.useranaly;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

public class FansOfSingerAddCity {
	private final static String TAB = "\t";
	private static class FansMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private static HashMap<String, HashSet<String>> uMap = new HashMap<String, HashSet<String>>();
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			File fans = new File("fansofsinger");
			BufferedReader br = new BufferedReader(new FileReader(fans));
			String line = "";
			while ((line = br.readLine()) != null) {
				String[] lpart = line.split(TAB);
				if (5 != lpart.length) continue;
				if (lpart[0].matches("211119691|5890")) {
					if (!uMap.containsKey(lpart[1])) {
						HashSet<String> tmpSet = new HashSet<String>();
						tmpSet.add(lpart[0]);
						uMap.put(lpart[1], tmpSet);
					} else {
						HashSet<String> tmpSet = uMap.get(lpart[1]);
						tmpSet.add(lpart[0]);
						uMap.put(lpart[1], tmpSet);
					}
				}
			}
			br.close();
			System.out.println("uMap.size() = " + uMap.size());
		}

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] lparts = line.split(TAB);
			if (11 != lparts.length) return;

			String baiduid = lparts[0];
			if (!uMap.containsKey(baiduid)) return;
			HashSet<String> singerSet = uMap.get(baiduid); 
			
			String play_json = lparts[2];
			JSONObject play = JSON.parseObject(play_json);
			for (Entry<String, Object> entry : play.entrySet()) {
				String getValue = entry.getValue().toString();
				String[] arr = getValue.split(TAB);
				if (10 != arr.length) continue;
				//{logDate\singer_id\p100ms\p60s\pend\time\favor\ref\rate\event_userid}
				String singer_id = arr[1];
				String p60s = arr[3];
				if (p60s.equals("1")) {
					for (String sid : singerSet) {
						context.write(new Text(sid + TAB + baiduid + TAB + singer_id), new IntWritable(1));
					}
				}
			}
		}
	}

	private static class FansReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable val : values) {
				count += val.get();
			}
			context.write(key, new IntWritable(count));
		}
	}

	public static boolean runMapReducue(Configuration conf, String input)
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		FileSystem hdfs = FileSystem.get(conf);
		String output = "/user/work/evan/tmp/FansOfSingerAddCity";
		hdfs.delete(new Path(output), true);

		Job job = Job.getInstance(conf);
		job.setJarByClass(FansOfSingerAddCity.class);
		job.setJobName("Evan_FansOfSingerAddCity");
		job.setNumReduceTasks(1);
		job.setMapperClass(FansMapper.class);
		job.setCombinerClass(FansReduce.class);
		job.setReducerClass(FansReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.addCacheFile(new URI("/user/work/evan/tmp/StatFansOfSinger/part-r-00000#fansofsinger"));
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, new Path(output));
		return job.waitForCompletion(true);
	}

	public static void main(String[] args)
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();

		if (args.length == 0) {
			System.err.println("Usage: class <in> <queue>");
			System.exit(1);
		}

		String queue = "mapred";
		if (args.length > 1) {
			queue = args[1].matches("hql|dstream|mapred|udw|user|common") ? args[1] : "mapred";
		}
		conf.set("mapreduce.job.queuename", queue);

		FansOfSingerAddCity.runMapReducue(conf, args[0]);
	}
}