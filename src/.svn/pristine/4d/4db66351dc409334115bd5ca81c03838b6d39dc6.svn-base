package com.baidumusic.vipuser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AddClassType {
	private static final String TAB = "\t";
	private static final String SPACE = " ";
	private static final Pattern COLON = Pattern.compile(":");
	private static DecimalFormat df = new DecimalFormat("0.000000");

	private static double atof(String s) {
		return Double.valueOf(s).doubleValue();
	}

	private static class GetMinMaxMap extends Mapper<LongWritable, Text, IntWritable, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] lparts = line.split(TAB);
			int len = lparts.length;
			/*
			 * 000103167167795AA7C03B58B7F62C2D 1 1 1.000000 0.000000 0.000000 3
			 * 3 1 1 1.0 1 0.0 0.375000 0.250000 0.375000 0.666667 3 2 0 3 2 2 1
			 * 1.5 1 0.5 2 3 1 3.0 3 0.0 3 0 0 0 0 8 0 0 0 3 3 1 1 1.0 1 0.0 1
			 * 1.000000 0.000000 0.000000 0.000000 1.000000 0 0 0 0 0 0 0 0 0 0
			 * 0 8 0 0 0 0 0 0.000000 1.000000 0.000000 0.000000 1.000000
			 * 0.000000 1 1 1 1 1.0 1 0.0 1 1.000000 2 2 1 1 1.0 1 0.0 1 0 0
			 */

			if (len < 3)
				return;
			StringBuilder sb = new StringBuilder();
			for (int i = 3; i < len; i++) {
				sb.append(lparts[i]).append(":").append(lparts[i]).append(SPACE);
			}
			context.write(new IntWritable(len - 3), new Text(sb.toString()));
		}
	}

	private static class GetMinMaxReduce extends Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int length = key.get();
			String[] strArr = new String[length];
			for (int i = 0; i < length; i++) {
				strArr[i] = Integer.MAX_VALUE + ":" + 0;
			}

			for (Text val : values) {
				String line = val.toString();
				StringTokenizer st = new StringTokenizer(line, " \t\n");
				int idx = 0;
				while (st.hasMoreTokens()) {
					String[] arrsplit = COLON.split(strArr[idx++]);
					double cur_min = atof(arrsplit[0]);
					double cur_max = atof(arrsplit[1]);
					String[] split = COLON.split(st.nextToken(), 2);
					if (split == null || split.length < 2) {
						throw new RuntimeException("Wrong input format at line " + line);
					}

					try {
						double min = atof(split[0]);
						double max = atof(split[1]);
						if (min < cur_min) {
							cur_min = min;
						}

						if (max > cur_max) {
							cur_max = max;
						}

						strArr[idx - 1] = cur_min + ":" + cur_max;
					} catch (NumberFormatException e) {
						throw new RuntimeException("Wrong input format at line " + line, e);
					}
				}
			}

			StringBuilder sb = new StringBuilder();
			sb.append(strArr[0]);
			for (int i = 1; i < length; i++) {
				sb.append(SPACE).append(strArr[i]);
			}
			context.write(key, new Text(sb.toString()));
		}
	}

	private static class ScaleMap extends Mapper<LongWritable, Text, Text, NullWritable> {
		private static String[] strArr;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			File file = new File("min.max");
			BufferedReader br = new BufferedReader(new FileReader(file));

			String line = "";
			while ((line = br.readLine()) != null) {
				String[] lpart = line.split(TAB);
				int length = Integer.valueOf(lpart[0]);
				strArr = new String[length];
				String vals = lpart[1];
				String[] vpart = vals.split(SPACE);
				for (int i = 0; i < vpart.length; i++) {
					strArr[i] = vpart[i];
				}
			}
			br.close();
			System.out.println("strArr.length = " + strArr.length);
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] lparts = line.split(TAB);
			int len = lparts.length;
			/*
			 * 000103167167795AA7C03B58B7F62C2D 1 1 1.000000 0.000000 0.000000 3
			 * 3 1 1 1.0 1 0.0 0.375000 0.250000 0.375000 0.666667 3 2 0 3 2 2 1
			 * 1.5 1 0.5 2 3 1 3.0 3 0.0 3 0 0 0 0 8 0 0 0 3 3 1 1 1.0 1 0.0 1
			 * 1.000000 0.000000 0.000000 0.000000 1.000000 0 0 0 0 0 0 0 0 0 0
			 * 0 8 0 0 0 0 0 0.000000 1.000000 0.000000 0.000000 1.000000
			 * 0.000000 1 1 1 1 1.0 1 0.0 1 1.000000 2 2 1 1 1.0 1 0.0 1 0 0
			 */

			if (len < 3)
				return;
			String baiduid = lparts[0];
			String vip_label = lparts[1];
			String sm_label = lparts[2];
			StringBuilder sb = new StringBuilder();
			sb.append(baiduid).append(SPACE).append(vip_label).append(SPACE).append(sm_label);
			for (int i = 3; i < len; i++) {
				double feature = atof(lparts[i]);
				double min = atof(strArr[i - 3].split(":")[0]);
				double max = atof(strArr[i - 3].split(":")[1]);
				String newval = df.format((max - min == 0) ? feature
						: ((feature - min == 0 && feature > 0) ? 1.0 / (max - min) : (feature - min) / (max - min)));
				sb.append(SPACE).append(newval);
			}
			context.write(new Text(sb.toString()), NullWritable.get());
		}
	}

	private static class LibsvmDataSetMap extends Mapper<LongWritable, Text, Text, NullWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] lparts = line.split(SPACE);
			int len = lparts.length;
			if (len < 3)
				return;
			StringBuilder sb = new StringBuilder();
			sb.append(lparts[0]).append(SPACE).append(lparts[1]).append(SPACE).append(lparts[2]);
			for (int i = 3; i < len; i++) {
				double val = Double.valueOf(lparts[i]);
				if (val > 0) {
					sb.append(SPACE).append(i - 2).append(":").append(df.format(val));
				}
			}
			String ret = sb.toString().trim();
			if (ret.length() > 0 && ret.contains(":")) {
				context.write(new Text(ret), NullWritable.get());
			}
		}
	}

	private static class CountFeatureMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] lparts = line.split(SPACE);
			int len = lparts.length;
			if (len < 4)
				return;
			// 000103167167795AA7C03B58B7F62C2D 1 1 1:2.000000
			for (int i = 3; i < len; i++) {
				String val = lparts[i].split(":")[0];
				context.write(new Text(val), new IntWritable(1));
			}
		}
	}

	private static class CountFeatureReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable val : values) {
				count += val.get();
			}
			context.write(key, new IntWritable(count));
		}
	}

	private static class CountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		IntWritable one = new IntWritable(1);

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] lparts = line.split(SPACE);
			if (lparts.length < 3)
				return;
			String vip = lparts[1];
			String shoumai = lparts[2];
			context.write(new Text("vip" + TAB + vip), one);
			context.write(new Text("shoumai" + TAB + shoumai), one);
		}
	}

	private static class CountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable val : values) {
				count += val.get();
			}
			context.write(key, new IntWritable(count));
		}
	}

	private static boolean runLoadMapReducue(Configuration conf, String input)
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Path outPath0 = new Path("/user/work/evan/output/GetMinMaxDataSet");
		Path outPath1 = new Path("/user/work/evan/output/ScaleDataSet");
		Path outPath2 = new Path("/user/work/evan/output/GenerateLibsvmDataSet");
		Path outPath3 = new Path("/user/work/evan/output/CountLibsvmFeature");
		Path outPath4 = new Path("/user/work/evan/output/CountLibsvmDataSet");

		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(outPath0, true);
		hdfs.delete(outPath1, true);
		hdfs.delete(outPath2, true);
		hdfs.delete(outPath3, true);
		hdfs.delete(outPath4, true);

		Job job = Job.getInstance(conf, "Evan_GetMinMaxDataSet");
		job.setJarByClass(AddClassType.class);
		job.setMapperClass(GetMinMaxMap.class);
		job.setCombinerClass(GetMinMaxReduce.class);
		job.setReducerClass(GetMinMaxReduce.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, outPath0);
		job.setNumReduceTasks(1);
		job.waitForCompletion(true);

		job = Job.getInstance(conf, "Evan_ScaleDataSet");
		job.setJarByClass(AddClassType.class);
		job.setMapperClass(ScaleMap.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.addCacheFile(new URI(outPath0 + "/part-r-00000#min.max"));
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, outPath1);
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		job.setNumReduceTasks(0);
		job.waitForCompletion(true);

		job = Job.getInstance(conf, "Evan_GenerateLibsvmDataSet");
		job.setJarByClass(AddClassType.class);
		job.setMapperClass(LibsvmDataSetMap.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, outPath1);
		FileOutputFormat.setOutputPath(job, outPath2);
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		job.setNumReduceTasks(100);
		job.waitForCompletion(true);

		job = Job.getInstance(conf, "Evan_CountLibsvmFeature");
		job.setJarByClass(AddClassType.class);
		job.setMapperClass(CountFeatureMap.class);
		job.setCombinerClass(CountFeatureReduce.class);
		job.setReducerClass(CountFeatureReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);
		FileInputFormat.setInputPaths(job, outPath2.toString());
		FileOutputFormat.setOutputPath(job, outPath3);
		job.waitForCompletion(true);

		job = Job.getInstance(conf, "Evan_CountLibsvmDataSet");
		job.setJarByClass(AddClassType.class);
		job.setMapperClass(CountMap.class);
		job.setCombinerClass(CountReduce.class);
		job.setReducerClass(CountReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);
		FileInputFormat.setInputPaths(job, outPath1);
		FileOutputFormat.setOutputPath(job, outPath4);
		return job.waitForCompletion(true);

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		if (args.length == 0) {
			System.err.println("Usage: AddClassType <in>");
			System.exit(1);
		}

		String queue = "mapred";
		if (args.length > 1) {
			queue = args[1].matches("hql|dstream|mapred|udw|user|common") ? args[1] : "mapred";
		}
		conf.set("mapreduce.job.queuename", queue);

		AddClassType.runLoadMapReducue(conf, args[0]);
	}
}