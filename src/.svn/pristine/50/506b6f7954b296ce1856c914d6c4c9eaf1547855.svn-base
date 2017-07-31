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
import java.util.TreeSet;

public class StatUserPlayDist {
	private final static String TAB = "\t";
	private static class UserMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] lparts = line.split(TAB);
			if (11 != lparts.length) return;
			String baiduid = lparts[0];
			String play_json = lparts[2];
			String orders = lparts[3];
			context.write(new Text(baiduid), new Text(play_json + TAB + orders));
		}
	}

	private static class UserReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// songid {play_list}
			int count = 0;
			int dist_count = 0;
			TreeSet<String> typeSet = new TreeSet<String>();
			try {
				for (Text val : values) {
					dist_count += 1;
					String[] vpart = val.toString().split(TAB);
					String play_json = vpart[0];
					String order_info = vpart[1];
					String[] ods_part = order_info.split(";");
					for (String ods : ods_part) {
						typeSet.add(ods.split("\\|")[0]);
						if (typeSet.size() > 1000) break;
					}
					JSONObject play = JSON.parseObject(play_json);
					count += play.size();
				}
			} catch (JSONException e) {
				// TODO nothing..
			} catch (ClassCastException e) {
				// TODO nothing..
			}
			String user_type = "";
			user_type = typeSet.toString().replace(" ", "");
			user_type = user_type.substring(1, user_type.length()-1);
			context.write(key, new Text(dist_count + TAB + count + TAB + user_type));
		}
	}
	
	private static class CountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		
		public String getUserType(String type) {
			String res = "no_login";
			if (type.equals("no_login")) {
				res = "no_login";
			} else if (type.equals("no_login,no_order")) {
				res = "no_login,no_order";
			} else if (type.equals("no_order")) {
				res = "no_order";
			} else if (type.contains("vip_order")) {
				res = "vip_order";
			} else {
				res = "other";
			}
			return res;
		}
		
		Text kText = new Text();
		Text akText = new Text();
		LongWritable vInt = new LongWritable(1);
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//12444652 1	2	no_login
			String[] lparts = value.toString().split(TAB);
			if (4 == lparts.length) {
				int cnt = Integer.valueOf(lparts[1]);
				String type = getUserType(lparts[3]);
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
	
	private static class TypeMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//12444652 1	2	no_login
			String[] lparts = value.toString().split(TAB);
			if (4 == lparts.length) {
				String type = lparts[3];
				context.write(new Text("all"), new Text("1" + TAB + lparts[1] + TAB + lparts[2]));
				context.write(new Text(type), new Text("1" + TAB + lparts[1] + TAB + lparts[2]));
			}
		}
	}
	
	private static class TypeReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			long dist = 0;
			long count = 0;
			long cnt_dist = 0;
			for (Text val : values) {
				String[] vparts = val.toString().split(TAB);
				if (3 == vparts.length) {
					cnt_dist += Integer.valueOf(vparts[0]);
					dist += Integer.valueOf(vparts[1]);
					count += Integer.valueOf(vparts[2]);
				}
			}
			context.write(key, new Text(cnt_dist + TAB + dist + TAB + count));
		}
	}
	
	public static boolean runLoadMapReducue(Configuration conf, String input)
			throws IOException, ClassNotFoundException, InterruptedException {
		String output1 = "/user/work/evan/tmp/StatUserPlayDist1";
		String output2 = "/user/work/evan/tmp/StatUserPlayDist2";
		String output3 = "/user/work/evan/tmp/StatUserPlayDist3";
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(new Path(output1), true);
		hdfs.delete(new Path(output2), true);
		hdfs.delete(new Path(output3), true);
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(StatUserPlayDist.class);
		job.setJobName("Evan_StatUserPlayDist-L1");
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
		job.setJarByClass(StatUserPlayDist.class);
		job.setJobName("Evan_StatUserPlayDist-L2");
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
		job.setJarByClass(StatUserPlayDist.class);
		job.setJobName("Evan_StatUserPlayDist-L3");
		job.setNumReduceTasks(1);
		job.setMapperClass(TypeMapper.class);
		job.setCombinerClass(TypeReduce.class);
		job.setReducerClass(TypeReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, output1);
		FileOutputFormat.setOutputPath(job, new Path(output3));
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

		StatUserPlayDist.runLoadMapReducue(conf, args[0]);
	}
}