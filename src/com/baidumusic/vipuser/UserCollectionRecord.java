package com.baidumusic.vipuser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hive.hcatalog.rcfile.RCFileMapReduceInputFormat;
import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.net.URLDecoder;
import org.apache.hadoop.fs.FileSystem;

public class UserCollectionRecord {
	private static class RcFileMapper extends Mapper<Object, BytesRefArrayWritable, Text, NullWritable> {	
		protected void map(Object key, BytesRefArrayWritable value, Context context)
				throws IOException, InterruptedException {
			Text txt = new Text();
			BytesRefWritable bid = value.get(1);
			txt.set(bid.getData(), bid.getStart(), bid.getLength());
			String baiduid = txt.toString();
			if (baiduid == null || !baiduid.matches("[0-9a-zA-Z]+")) return;
			
			BytesRefWritable date = value.get(20);
			txt.set(date.getData(), date.getStart(), date.getLength());
			String logDate = txt.toString();
			if (logDate == null || logDate.length() == 0) {
				logDate = "2048-00-00 00:00:00";
			}
			
			BytesRefWritable request = value.get(21);
			txt.set(request.getData(), request.getStart(), request.getLength());
			JSONObject json = Tools.splitMap(txt.toString());
			if (json.isEmpty()) return;
			
			String[] singers = {"artistid", "singer_id"};
			String[] songs = {"sid", "song_id", "songid", "songId"};
			String type = json.containsKey("type") ? (String) json.get("type") : "";
			String click = type.matches("click_favor|click_singleFavor") ? "1" : "0";
			String singer = "default";
			for (String sin : singers) {
				if (json.containsKey(sin)) {
					singer = sin;
				}
			}
			String singer_id = singer.equals("default") ? "default" : (String) json.get(singer);
			
			String song = "000000000";
			for (String so : songs) {
				if (json.containsKey(so)) {
					song = so;
				}
			}
			String song_id = song.equals("000000000") ? "000000000" : (String) json.get(song);
			
			context.write(new Text(baiduid + "\t" + logDate + "\t" + URLDecoder.decode(singer_id, "UTF-8") 
					+ "\t" + song_id + "\t" + click), 
					NullWritable.get());
		}
	}

	private static class RcFileReduce extends Reducer<Text, NullWritable, Text, NullWritable> {
		protected void reduce(Text key, Iterable<NullWritable> values, Context context) 
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	public static boolean runLoadMapReducue(Configuration conf, Path input, Path output) 
			throws IOException, ClassNotFoundException, InterruptedException {
		conf.set("hive.io.file.read.all.columns", "false");
		conf.set("hive.io.file.readcolumn.ids", "1,20,21");
		Job job = Job.getInstance(conf);
		job.setJarByClass(UserCollectionRecord.class);
		job.setJobName("Evan_UserCollectionRecord");
		job.setNumReduceTasks(1);
		job.setMapperClass(RcFileMapper.class);
		job.setCombinerClass(RcFileReduce.class);
		job.setReducerClass(RcFileReduce.class);
		job.setInputFormatClass(RCFileMapReduceInputFormat.class);
		RCFileMapReduceInputFormat.addInputPath(job, input);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileOutputFormat.setOutputPath(job, output);
		
		return job.waitForCompletion(true);
	}

	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {
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
		String out = "/user/work/evan/tmp/UserCollectionRecord";
		Path path = new Path(out);
		hdfs.delete(path, true);
		  
		UserCollectionRecord.runLoadMapReducue(conf, new Path(args[0]), new Path(out));
	}
}
