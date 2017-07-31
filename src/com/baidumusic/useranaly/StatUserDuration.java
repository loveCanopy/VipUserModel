package com.baidumusic.useranaly;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map.Entry;
import java.util.TreeSet;

public class StatUserDuration {
	private final static String TAB = "\t";
	private static class UserMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			String line = value.toString();
			int idx = line.indexOf(TAB);
			if (idx > 0) {
				context.write(new Text(line.substring(0, idx)), new Text(line.substring(idx + 1)));
			}
		}
	}

	private static class UserReduce extends Reducer<Text, Text, Text, Text> {
		private String getTwoDay(String sj1, String sj2) {
	        SimpleDateFormat myFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	        long day = 0;
	        try {
	            Date date = myFormatter.parse(sj1);
	            Date mydate = myFormatter.parse(sj2);
	            day = (date.getTime() - mydate.getTime()) / (24 * 60 * 60 * 1000);
	        } catch (Exception e) {
	            return "0";
	        }
	        return day + "";
	    }

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// note: baiduid song_id {logDate\singer_id\p100ms\p60s\pend\time\favor\ref\rate\event_userid} order_info
			// note: singer_id\style\language\publish_time\category\si_has_filmtv
			// note: date-hour\device_type\product\refer_domain
			String first_seen = "9999-99-99 99:99:99";
			String last_seen = "0000-00-00 00:00:00";
			TreeSet<String> typeSet = new TreeSet<String>();
			for (Text val : values) {
				String[] vparts = val.toString().split(TAB);
				if (10 != vparts.length) continue;
				String play_json = vparts[1];
				String order_info = vparts[2];
				
				String[] ods_part = order_info.split(";");
				for (String ods : ods_part) {
					typeSet.add(ods.split("\\|")[0]);
					if (typeSet.size() > 1000) break;
				}
				try {
					JSONObject play = JSON.parseObject(play_json);
					for (Entry<String, Object> entry : play.entrySet()) {
						String date = entry.getKey();
						if (date.compareTo(first_seen) < 0) {
							first_seen = date;
						}
						if (date.compareTo(last_seen) > 0) {
							last_seen = date;
						}
					}
				} catch (JSONException e) {
					// TODO nothing..
				} catch (ClassCastException e) {
					// TODO nothing..
				}
			}
			String gap = getTwoDay(last_seen, first_seen);
			String user_type = "";
			user_type = typeSet.toString().replace(" ", "");
			user_type = user_type.substring(1, user_type.length()-1);
			if (Integer.valueOf(gap) > -1) {
				context.write(new Text(user_type), new Text(gap));
			}
		}
	}
	
	private static class TimeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(value, new IntWritable(1));
		}
	}
	
	private static class TimeReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
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
			throws IOException, ClassNotFoundException, InterruptedException {
		String output1 = "/user/work/evan/tmp/StatUserDuration1";
		String output2 = "/user/work/evan/tmp/StatUserDuration2";
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(new Path(output1), true);
		hdfs.delete(new Path(output2), true);
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(StatUserDuration.class);
		job.setJobName("Evan_StatUserDuration-L1");
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
		job.setJarByClass(StatUserDuration.class);
		job.setJobName("Evan_StatUserDurationo-L2");
		job.setNumReduceTasks(1);
		job.setMapperClass(TimeMapper.class);
		job.setCombinerClass(TimeReduce.class);
		job.setReducerClass(TimeReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

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

		StatUserDuration.runLoadMapReducue(conf, args[0]);
	}
}