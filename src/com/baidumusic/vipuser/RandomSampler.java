package com.baidumusic.vipuser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RandomSampler {
	private static final String SPACE = new String(" ");

	private static class Map_1 extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] lparts = line.split(SPACE);
			if (lparts.length < 3)
				return;
			String baiduid = lparts[0];
			String vip_label = lparts[1];
			String sm_label = lparts[2];
			context.write(new Text("vip_" + vip_label), new Text(baiduid));
			context.write(new Text("shoumai_" + sm_label), new Text(baiduid));
		}
	}

	private static class Reduce_1 extends Reducer<Text, Text, Text, NullWritable> {
		private static final int randNum_1 = 60000;
		private static final int randNum_2 = 10000;
		private static final int randNum_3 = 34000;
		
		private static final int randNum_4 = 10000;
		private static final int randNum_5 = 10000;
		private static final int randNum_6 = 16000;
		
		private static final int randNum_7 = 10000;
		private static final int randNum_8 = 16000;
		private static final int randNum_9 = 10000;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			{
				String type = key.toString();
				int randNum = 1;
				if (type.endsWith("_1")) {
					randNum = randNum_1;
				} else if (type.endsWith("_2")) {
					randNum = randNum_2;
				} else if (type.endsWith("_3")) {
					randNum = randNum_3;
				} else if (type.endsWith("vip_4")) {
					randNum = randNum_4;
				} else if (type.endsWith("vip_5")) {
					randNum = randNum_5;
				} else if (type.endsWith("vip_6")) {
					randNum = randNum_6;
				} else if (type.equals("shoumai_4")) {
					randNum = randNum_7;
				} else if (type.equals("shoumai_5")) {
					randNum = randNum_8;
				} else if (type.equals("shoumai_6")) {
					randNum = randNum_9;
				}
				Random rRandom = new Random();
				String[] vecData = new String[randNum];
				int nCurrentIndex = 0;
				for (Text val : values) {
					String tValue = val.toString();
					if (nCurrentIndex < randNum) {
						vecData[nCurrentIndex] = tValue;
					} else {
						int rnumber = rRandom.nextInt(nCurrentIndex + 1);
						if (rnumber < randNum) {
							vecData[rnumber] = tValue;
						}
					}
					nCurrentIndex++;
				}
				/*
				 * String[] tokens = line.split("-"); mos.write("MOSInt",new
				 * Text(tokens[0]), new IntWritable(1)); //
				 * 直接输出至：/home/mr/wuyunyun/output/MOSInt-m-00000
				 * mos.write("MOSText", new Text(tokens[0]),tokens[2]); //
				 * 直接输出至：/home/mr/wuyunyun/output/MOSText-m-00000
				 * mos.write("MOSText", new
				 * Text(tokens[0]),line,tokens[0]+"/part"); //
				 * 输出至指定的目录下：/home/mr/wuyunyun/output/xxx/part-m-00000
				 */
				for (int i = 0; i < vecData.length; i++) {
					if (vecData[i] != null && vecData[i].length() > 0) {
						context.write(new Text(type.split("_")[0] + "\t" + vecData[i]), NullWritable.get());
					}
				}
			}
		}
	}

	private static class TrainMapper extends Mapper<LongWritable, Text, Text, Text> {

		private MultipleOutputs<Text, Text> mos;
		private static HashMap<String, ArrayList<String>> hMap = new HashMap<String, ArrayList<String>>();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			mos = new MultipleOutputs<Text, Text>(context);
			for (int i = 0; i < 5; i++) {
				String filename = "sample" + i + ".baiduid";
				File file = new File(filename);
				BufferedReader br = new BufferedReader(new FileReader(file));
				String line = "";
				while ((line = br.readLine()) != null) {
					String[] lpart = line.split("\t");
					if (2 == lpart.length) {
						String label = lpart[0];
						String baiduid = lpart[1];
						if (hMap.containsKey(baiduid)) {
							ArrayList<String> tmpList = hMap.get(baiduid);
							tmpList.add(label);
							hMap.put(baiduid, tmpList);
						} else {
							ArrayList<String> tmpList = new ArrayList<String>();
							tmpList.add(label);
							hMap.put(baiduid, tmpList);
						}
					}
				}
				br.close();
			}
			System.out.println("hMap.size() = " + hMap.size());
		}

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] lparts = line.split(SPACE);
			if (lparts.length < 4)
				return;
			String baiduid = lparts[0];
			if (hMap.containsKey(baiduid)) {
				ArrayList<String> tmpList = hMap.get(baiduid);
				if (tmpList.contains("vip")) {
					mos.write("vip", new Text(line), null, "vip/part");
				}
				if (tmpList.contains("shoumai")) {
					mos.write("shoumai", new Text(line), null, "shoumai/part");
				}
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			System.err.println("Usage: RandomSampler <in> <queue>");
			System.exit(1);
		}

		String queue = "mapred";
		if (args.length > 1) {
			queue = args[1].matches("hql|dstream|mapred|udw|user|common") ? args[1] : "mapred";
		}
		Configuration conf = new Configuration();
		conf.set("mapreduce.job.queuename", queue);

		Path outPath1 = new Path("/user/work/evan/tmp/RandomSampler");
		Path outPath2 = new Path("/user/work/evan/tmp/GetTrainSet");
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(outPath1, true);
		hdfs.delete(outPath2, true);

		Job job = Job.getInstance(conf, "Evan_RandomSampler");
		job.setJarByClass(RandomSampler.class);
		job.setMapperClass(Map_1.class);
		job.setReducerClass(Reduce_1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, outPath1);
		job.setNumReduceTasks(5);
		job.waitForCompletion(true);

		job = Job.getInstance(conf);
		job.setJarByClass(RandomSampler.class);
		job.setJobName("Evan_GetTrainSet");
		job.setMapperClass(TrainMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.addCacheFile(new URI(outPath1 + "/part-r-00000#sample0.baiduid"));
		job.addCacheFile(new URI(outPath1 + "/part-r-00001#sample1.baiduid"));
		job.addCacheFile(new URI(outPath1 + "/part-r-00002#sample2.baiduid"));
		job.addCacheFile(new URI(outPath1 + "/part-r-00003#sample3.baiduid"));
		job.addCacheFile(new URI(outPath1 + "/part-r-00004#sample4.baiduid"));
		job.setNumReduceTasks(1);
		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, outPath2);
		MultipleOutputs.addNamedOutput(job, "vip", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "shoumai", TextOutputFormat.class, Text.class, Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
