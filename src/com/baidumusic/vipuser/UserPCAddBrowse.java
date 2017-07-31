package com.baidumusic.vipuser;

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
import java.util.HashSet;

public class UserPCAddBrowse {
	private static class AddMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			InputSplit inputSplit = (InputSplit) context.getInputSplit();
			String pathName = ((FileSplit) inputSplit).getPath().toString();
			String line = value.toString();
			// /user/work/evan/tmp/UserBrowseRecord
			// FFFF2D24911467517D74E1EF8AC76590
			// {"20160813-15\tpcweb\tmusic\twww.baidu.com":1}
			// /user/work/evan/tmp/UserAddSongInfo2
			// baiduid songid {...} \t * 7
			if (pathName.contains("UserAddSongInfo")) {
				int idx = line.indexOf("\t");
				if (idx > 0 && idx < line.length()) {
					context.write(new Text(line.substring(0, idx)), new Text(line));
				}
			} else if (pathName.contains("UserBrowseRecord")) {
				int idx = line.indexOf("\t");
				if (idx > 0 && idx < line.length()) {
					context.write(new Text(line.substring(0, idx)), new Text(line.substring(idx + 1)));
				}
			}
		}
	}

	private static class AddReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			HashSet<String> hSet = new HashSet<String>();
			String browse = "null";
			for (Text val : values) {
				if (val.toString().split("\t").length > 2) {
					hSet.add(val.toString());
				} else {
					browse = val.toString();
				}
			}
			if (hSet.size() > 0) {
				for (String line : hSet) {
					context.write(new Text(line), new Text(browse));
				}
			}
		}
	}

	public static boolean runLoadMapReducue(Configuration conf, String input, Path output) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(UserPCAddBrowse.class);
		job.setJobName("Evan_UserPCAddBrowse");
		job.setNumReduceTasks(1000);
		job.setMapperClass(AddMapper.class);
		job.setReducerClass(AddReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		return job.waitForCompletion(true);
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, 
			InterruptedException, URISyntaxException {
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
		String out = "/user/work/evan/tmp/UserPCAddBrowse";
		Path path = new Path(out);
		hdfs.delete(path, true);
		
		UserPCAddBrowse.runLoadMapReducue(conf, args[0], new Path(out));
	}
}