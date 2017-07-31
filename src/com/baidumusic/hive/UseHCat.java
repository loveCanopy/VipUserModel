package com.baidumusic.hive;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

public class UseHCat extends Configured implements Tool {
	public static class Map extends Mapper<WritableComparable<Object>, HCatRecord, Text, IntWritable> {
		protected void map( WritableComparable<Object> key,
				HCatRecord value,
				Mapper<WritableComparable<Object>, HCatRecord,
				Text, IntWritable>.Context context)
						throws IOException, InterruptedException {
			// The group table from /etc/group has name, 'x', id
			
			//HCatSchema schema = HCatBaseInputFormat.getTableSchema(context.getConfiguration());
			//groupname = (String) value.get("groupname", schema);
			//int id = (Integer) value.get("id", schema);
			
			String groupname = (String) value.get(0);
			int id = (Integer) value.get(2);
			// Just select and emit the name and ID
			context.write(new Text(groupname), new IntWritable(id));
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, WritableComparable<Object>, HCatRecord> {
		protected void reduce( Text key,
				Iterable<IntWritable> values,
				Reducer<Text, IntWritable,
				WritableComparable<Object>, HCatRecord>.Context context)
						throws IOException, InterruptedException {
			// Only expecting one ID per group name
			Iterator<IntWritable> iter = values.iterator();
			IntWritable iw = iter.next();
			int id = iw.get();
			// Emit the group name and ID as a record
			HCatRecord record = new DefaultHCatRecord(2);
			record.set(0, key.toString());
			record.set(1, id);
			context.write(null, record);
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		args = new GenericOptionsParser(conf, args).getRemainingArgs();

		// Get the input and output table names as arguments
		String inputTableName = args[0];
		String outputTableName = args[1];
		// Assume the default database
		String dbName = "udw_event";

		Job job = Job.getInstance(conf);
		job.setJobName("UseHCat");
		HCatInputFormat.setInput(job, dbName, inputTableName);
		job.setJarByClass(UseHCat.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// An HCatalog record as input
		job.setInputFormatClass(HCatInputFormat.class);

		// Mapper emits a string as key and an integer as value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// Ignore the key for the reducer output; emitting an HCatalog record as value
		job.setOutputKeyClass(WritableComparable.class);
		job.setOutputValueClass(DefaultHCatRecord.class);
		job.setOutputFormatClass(HCatOutputFormat.class);

		HCatOutputFormat.setOutput(job, OutputJobInfo.create(dbName, outputTableName, null));

		HCatSchema s = HCatOutputFormat.getTableSchema(conf);
		System.err.println("INFO: output schema explicitly set for writing:" + s);
		HCatOutputFormat.setSchema(job, s);
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new UseHCat(), args);
		System.exit(exitCode);
	}
}