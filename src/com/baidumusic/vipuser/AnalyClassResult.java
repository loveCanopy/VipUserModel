package com.baidumusic.vipuser;

import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnalyClassResult {
	private static final String TAB = new String("\t");

	private static class Map_1 extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//000130B4D8BA81C18C3CBFCC34660009	1	1.0	0.545700	0.255181	0.111931	0.087188; 1	1.0	0.550526	0.261348    0.100406	0.087720
			String line = value.toString();
			String vipart = line.split(";")[0];
			String[] lparts = vipart.split(TAB);
			if (lparts.length < 3)
				return;
			String vip_label = lparts[1];
			String predict_label = lparts[2];
			String score = lparts[Double.valueOf(predict_label).intValue() + 2];
			Double d_s = new Double(Math.floor(Double.valueOf(score) * 10));
			context.write(new Text(vip_label + "\t" + predict_label + "\t" + d_s.intValue()), 
					new IntWritable(1));
		}
	}

	private static class Reduce_1 extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			{
				int count = 0;
				for (IntWritable val : values) {
					count += val.get();
				}
				context.write(key, new IntWritable(count));
			}
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// To do nothing
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			System.err.println("Usage: AnalyClassResult <in> <queue>");
			System.exit(1);
		}

		String queue = "mapred";
		if (args.length > 1) {
			queue = args[1].matches("hql|dstream|mapred|udw|user|common") ? args[1] : "mapred";
		}
		Configuration conf = new Configuration();
		conf.set("mapreduce.job.queuename", queue);

		Path outPath1 = new Path("/user/work/evan/tmp/AnalyClassResult");
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(outPath1, true);

		Job job = Job.getInstance(conf, "Evan_AnalyClassResult");
		job.setJarByClass(AnalyClassResult.class);
		job.setMapperClass(Map_1.class);
		job.setCombinerClass(Reduce_1.class);
		job.setReducerClass(Reduce_1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, outPath1);
		job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
