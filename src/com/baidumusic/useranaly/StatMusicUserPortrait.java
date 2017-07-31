package com.baidumusic.useranaly;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;

public class StatMusicUserPortrait {
	private final static String TAB = "\t";
	private static final ArrayList<String> ItemList = new ArrayList<String>();
	private static final HashMap<Integer, String> hMap = new HashMap<Integer, String>();
	static {
		ItemList.add("性别");
		ItemList.add("年龄");
		ItemList.add("星座");
		ItemList.add("消费水平");
		ItemList.add("教育水平");
		ItemList.add("所在行业");
		ItemList.add("资产状况");
		ItemList.add("人生阶段");
	}
	
	static {
		hMap.put(1, "性别");
		hMap.put(2, "年龄");
		hMap.put(3, "星座");
		hMap.put(4, "消费水平");
		hMap.put(5, "教育水平");
		hMap.put(6, "所在行业");
		hMap.put(7, "资产状况");
		hMap.put(8, "人生阶段");
	}

	private static String getPortraitInfo(String line) {
		HashMap<String, String> hMap = new HashMap<String, String>();
		String[] lpart = line.replaceAll(" ", "").split(",");
		for (String item : lpart) {
			String[] ipart = item.split("/|\\|");
			if (ipart.length == 3) {
				if (ItemList.contains(ipart[0])) {
					hMap.put(ipart[0], ipart[1]);
				}
			}
		}
		StringBuilder basic = new StringBuilder();
		int cnt = 0;
		for (String item : ItemList) {
			String val = hMap.containsKey(item) ? hMap.get(item) : "NAN";
			if (cnt++ == 0) {
				basic.append(val);
			} else {
				basic.append(TAB).append(val);
			}
		}

		return basic.toString();
	}

	private static class FansMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			InputSplit inputSplit = (InputSplit) context.getInputSplit();
			String pathName = ((FileSplit) inputSplit).getPath().toString();
			String line = value.toString();
			if (pathName.contains("music_erised_user_profile_collect")) {
				String[] lparts = line.split("\001");
				if (2 == lparts.length) {
					String portrait = getPortraitInfo(lparts[1]);
					context.write(new Text(lparts[0]), new Text(portrait));
				}
			} else {
				String[] lparts = line.split(TAB);
				if (lparts.length > 4) {
					String baiduid = lparts[0];
					String order_info = lparts[3];
					context.write(new Text(baiduid), new Text("TYPE#" + order_info));
				}
			}
		}
	}

	private static class FansReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String portrait = "NAN\tNAN\tNAN\tNAN\tNAN\tNAN\tNAN\tNAN";
			boolean Is_hit = false;
			TreeSet<String> typeSet = new TreeSet<String>();
			for (Text val : values) {
				if (val.toString().startsWith("TYPE#")) {
					String order_info = val.toString().split("TYPE#")[1];
					String[] ods_part = order_info.split(";");
					if (ods_part.length > 100) continue;
					for (String ods : ods_part) {
						typeSet.add(ods.split("\\|")[0]);
						if (typeSet.size() > 1000) break;
					}
					Is_hit = true;
				} else if (val.toString().length() > 0) {
					portrait = val.toString();
				}
			}
			if (Is_hit && typeSet.size() > 0) {
				String type = typeSet.toString().replace(" ", "");
				type = type.substring(1, type.length()-1);
				context.write(new Text("all"), new Text(portrait));
				context.write(new Text(type), new Text(portrait));
			}
		}
	}

	private static class GroupMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		protected void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] lparts = line.split(TAB);
			if (9 == lparts.length) {
				String type = lparts[0];
				for (int i=1; i< lparts.length; i++) {
					context.write(new Text(type + TAB + hMap.get(i) + TAB + lparts[i]), new LongWritable(1));
				}
			}
		}
	}

	private static class GroupReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
		protected void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long count = 0;
			for (LongWritable val : values) {
				count += val.get();
			}
			context.write(key, new LongWritable(count));
		}
	}

	public static boolean runMapReducue(Configuration conf, String input)
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		FileSystem hdfs = FileSystem.get(conf);
		String output1 = "/user/work/evan/tmp/StatMusicUserPortrait1";
		String output2 = "/user/work/evan/tmp/StatMusicUserPortrait2";
		hdfs.delete(new Path(output1), true);
		hdfs.delete(new Path(output2), true);

		Job job = Job.getInstance(conf);
		job.setJarByClass(StatMusicUserPortrait.class);
		job.setJobName("Evan_StatMusicUserPortrait-L1");
		job.setNumReduceTasks(2000);
		job.setMapperClass(FansMapper.class);
		job.setReducerClass(FansReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, new Path(output1));
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		job.waitForCompletion(true);

		job = Job.getInstance(conf);
		job.setJarByClass(StatMusicUserPortrait.class);
		job.setJobName("Evan_FansOfSingerAddProtrait-L2");
		job.setNumReduceTasks(1);
		job.setMapperClass(GroupMapper.class);
		job.setCombinerClass(GroupReduce.class);
		job.setReducerClass(GroupReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
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

		StatMusicUserPortrait.runMapReducue(conf, args[0]);
	}
}