package com.baidumusic.vipuser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hive.hcatalog.rcfile.RCFileMapReduceInputFormat;
import java.io.IOException;
import java.net.URISyntaxException;

public class NsclickParse {
	private static class NsclickMapper extends Mapper<Object, BytesRefArrayWritable, Text, NullWritable> {
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);

		}
		
		protected void map(Object key, BytesRefArrayWritable value, Context context)
				throws IOException, InterruptedException {
			Text txt = new Text();
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < value.size(); i++) {
				BytesRefWritable v = value.get(i);
				txt.set(v.getData(), v.getStart(), v.getLength());
				if (i == value.size() - 1) {
					sb.append(txt.toString());
				} else {
					sb.append(txt.toString() + "\t");
				}
			}
			String res = Click_ETL.parseLine(sb.toString());
			if (res.length() > 0) {
				context.write(new Text(res), NullWritable.get());
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
		}

	}

	private static class NsclickReduce extends Reducer<Text, NullWritable, Text, NullWritable> {
		protected void reduce(Text key, Iterable<NullWritable> values, Context context) 
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	public static boolean runLoadMapReducue(Configuration conf, Path input, Path output) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(NsclickParse.class);
		job.setJobName("Evan_NsclickParse");
		job.setNumReduceTasks(1);
		job.setMapperClass(NsclickMapper.class);
		job.setReducerClass(NsclickReduce.class);
		job.setInputFormatClass(RCFileMapReduceInputFormat.class);
		RCFileMapReduceInputFormat.addInputPath(job, input);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileOutputFormat.setOutputPath(job, output);
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		//FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
		return job.waitForCompletion(true);
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, 
	InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		conf.set("mapreduce.job.queuename", "mapred");

		if (args.length != 2) {
			System.err.println("Usage: NsclickParse <in> <out>");
			System.exit(2);
		}
		NsclickParse.runLoadMapReducue(conf, new Path(args[0]), new Path(args[1]));
	}
}