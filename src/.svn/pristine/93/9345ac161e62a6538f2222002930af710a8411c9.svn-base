package com.baidumusic.hive;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatBaseInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;

public class MRExample extends Configured implements Tool {

	@SuppressWarnings("rawtypes")
	public static class Map extends Mapper<WritableComparable, HCatRecord, Text, LongWritable> {
        protected void map(
                WritableComparable key,
                HCatRecord value,
                Mapper<WritableComparable, HCatRecord,
                        Text, LongWritable>.Context context)
                throws IOException, InterruptedException {
        	HCatSchema schema = HCatBaseInputFormat.getTableSchema(context.getConfiguration());
            String user = (String) value.get("user", schema);
            //String user = (String) value.get(1);
            context.write(new Text(user), new LongWritable(1));
        }
    }

	public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {
			protected void reduce(Text key, Iterable<LongWritable> values, Context context) 
              throws IOException, InterruptedException {
          int sum = 0;
          Iterator<LongWritable> iter = values.iterator();
          while (iter.hasNext()) {
              sum++;
              iter.next();
          }
          context.write(key, new LongWritable(sum));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        String inputTableName = "userid";
        String dbName = "default";
        Path outputfile = new Path(args[0]);
        
        Job job = Job.getInstance(conf);
        job.setJobName("Evan_MRExample");
        HCatInputFormat.setInput(job, dbName, inputTableName);
        //HCatInputFormat.setInput(job, InputJobInfo.create(dbName, inputTableName, "ds=\"20110924\""));
        
        
        // initialize HCatOutputFormat
        job.setInputFormatClass(HCatInputFormat.class);
        job.setJarByClass(MRExample.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        HCatSchema s = HCatOutputFormat.getTableSchema(conf);
        HCatOutputFormat.setSchema(job, s);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outputfile);
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MRExample(), args);
        System.exit(exitCode);
    }
}