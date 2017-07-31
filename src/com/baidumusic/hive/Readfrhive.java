package com.baidumusic.hive;

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
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashSet;

public class Readfrhive {
	public static String filterCol(int col, String todel) {
		String res = "";
		String[] nums = todel.split(",");
		HashSet<Integer> hSet = new HashSet<Integer>();
		for (String n : nums) {
			hSet.add(Integer.valueOf(n));
		}
		for (int i=0; i<col-1; i++) {
			if (!hSet.contains(i)) {
				res += i + ",";
			}
		}
		return res;
	}
	
	private static class RcFileMapper extends Mapper<Object, BytesRefArrayWritable, Text, NullWritable> {	
		protected void map(Object key, BytesRefArrayWritable value, Context context)
				throws IOException, InterruptedException {
			Text txt = new Text();
			//if (value.size() != 42) return;
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < value.size(); i++) {
				//if (i == 13 || i == 22 || i == 29) continue;
				BytesRefWritable v = value.get(i);
				/*
				if (i == 9) {
					txt.set(v.getData(), v.getStart(), v.getLength());
					byte[] bs = txt.getBytes();
					
					StringBuilder sbl = new StringBuilder();
					for (int j = 0; j < bs.length; j++) {
						sbl.append(bs[j]).append(',');
					}
					//context.write(new Text("Mark " + (bs[0] - '0') + (bs[1] - '0') + (bs[2] - '0')), NullWritable.get());
					sb.append(sbl.toString()).append("\t");
					continue;
				} */
				String res = "";
				txt.set(v.getData(), v.getStart(), v.getLength());
				if (txt == null || txt.toString() == null || txt.toString().length() == 0) {
					sb.append("NULL").append("\t");
					continue;
				}
				//res = new String(txt.getBytes(), 0, txt.getLength(), "UTF-8"); 
				res = txt.toString().replaceAll("\n", " ").replaceAll("\t", " ");
				if (i == value.size() - 1) {
					sb.append(res);
				} else {
					sb.append(res + "\t");
				}
			}
			//String line=new String(text.getBytes(),0,text.getLength(),"GBK"); 
			context.write(new Text(sb.toString()), NullWritable.get());
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
		//conf.set("hive.io.file.read.all.columns", "false");
		//conf.set("hive.io.file.readcolumn.ids", "8,9,10,");
		Job job = Job.getInstance(conf);
		job.setJarByClass(Readfrhive.class);
		job.setJobName("Evan_Readfrhive");
		job.setNumReduceTasks(1);
		job.setMapperClass(RcFileMapper.class);
		job.setCombinerClass(RcFileReduce.class);
		job.setReducerClass(RcFileReduce.class);
		job.setInputFormatClass(RCFileMapReduceInputFormat.class);
		//job.setInputFormat(RCFileInputFormat.class);
		//MultipleInputs.addInputPath(job, input, RCFileInputFormat.class);
		RCFileMapReduceInputFormat.addInputPath(job, input);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileOutputFormat.setOutputPath(job, output);
		
		return job.waitForCompletion(true);
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		
		if (args.length < 2) {
			System.err.println("Usage: rcfile <in> <out>");
			System.exit(2);
		}
		String queue = "mapred";
		if (args.length > 2) {
			queue = args[2].matches("hql|dstream|mapred|udw|user|common") ? args[2] : "mapred"; 
		}
		conf.set("mapreduce.job.queuename", queue);
		
		Readfrhive.runLoadMapReducue(conf, new Path(args[0]), new Path(args[1]));
	}
}