package com.baidumusic.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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

public class RcFileReaderJob {
	private static class RcFileMapper extends Mapper<Object, BytesRefArrayWritable, Text, NullWritable> {
		
		protected void map(Object key, BytesRefArrayWritable value, Context context)
				throws IOException, InterruptedException {
			Text txt = new Text();
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < value.size(); i++) {
				BytesRefWritable v = value.get(i);
				txt.set(v.getData(), v.getStart(), v.getLength());
				
				//long aaa = Tools.BytetoLong(txt.getBytes());
				
				String res = txt.toString().replaceAll("\n", " ");
				if (i == value.size() - 1) {
					sb.append(res);
				} else {
					sb.append(res + "\t");
				}
			}
			context.write(new Text(sb.toString()), NullWritable.get());
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
		}

		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);

		}
	}

	private static class RcFileReduce extends Reducer<Text, NullWritable, Text, NullWritable> {
		protected void reduce(Text key, Iterable<NullWritable> values, Context context) 
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	public static boolean runLoadMapReducue(Configuration conf, String input, Path output) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(RcFileReaderJob.class);
		job.setJobName("Evan_RcFileReaderJob");
		job.setNumReduceTasks(1);
		job.setMapperClass(RcFileMapper.class);
		job.setReducerClass(RcFileReduce.class);
		job.setInputFormatClass(RCFileMapReduceInputFormat.class);
		//job.setInputFormat(RCFileInputFormat.class);
		//MultipleInputs.addInputPath(job, input, RCFileInputFormat.class);
		//RCFileMapReduceInputFormat.addInputPath(job, input);
		RCFileMapReduceInputFormat.setInputPaths(job, input);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileOutputFormat.setOutputPath(job, output);
		
		return job.waitForCompletion(true);
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
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
		String out = "/user/work/evan/tmp/RcFileReaderJob";
		Path path = new Path(out);
		hdfs.delete(path, true);
		
		RcFileReaderJob.runLoadMapReducue(conf, args[0], new Path(out));
	}
}  