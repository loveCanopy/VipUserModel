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
import java.io.IOException;
import java.net.URISyntaxException;

public class StatQukuDist {
	private final static String TAB = "\t";
	private static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		IntWritable one = new IntWritable(1);
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] lparts = line.split(TAB);
			if (85 == lparts.length) {
				String style = lparts[8].length() > 0 ? lparts[8] : "unknown";
				String language = lparts[18].length() > 0 ? lparts[18] : "unknown";
				context.write(new Text("STYPE" + TAB + style), one);
				context.write(new Text("STYPE" + TAB + "all"), one);
				context.write(new Text("LANGUAGE" + TAB + language), one);
				context.write(new Text("LANGUAGE" + TAB + "all"), one);
				context.write(new Text("all"), one);
			}
		}
	}

	private static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable val : values) {
				count += val.get();
			}
			context.write(key, new IntWritable(count));
		}
	}
	
	public static boolean runLoadMapReducue(Configuration conf, String input) 
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		FileSystem hdfs = FileSystem.get(conf);
		String out = "/user/work/evan/tmp/StatQukuDist";
		hdfs.delete(new Path(out), true);
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(StatQukuDist.class);
		job.setJobName("Evan_StatQukuDist");
		job.setNumReduceTasks(1);
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReduce.class);
		job.setReducerClass(MyReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, new Path(out));
		return job.waitForCompletion(true);
	}

	public static void main(String[] args) throws IOException, 
			ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		
		if (args.length == 0) {
			System.err.println("Usage: class <in>");
			System.exit(1);
		}
		
		String queue = "mapred";
		if (args.length > 1) {
			queue = args[1].matches("hql|dstream|mapred|udw|user|common") ? args[1] : "mapred"; 
		}
		conf.set("mapreduce.job.queuename", queue);
		
		StatQukuDist.runLoadMapReducue(conf, args[0]);
	}
}