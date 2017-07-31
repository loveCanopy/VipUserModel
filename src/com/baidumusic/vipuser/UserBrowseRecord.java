package com.baidumusic.vipuser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hive.hcatalog.rcfile.RCFileMapReduceInputFormat;

import com.alibaba.fastjson.JSON;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;
import java.util.HashMap;

public class UserBrowseRecord {
	private final static String TAB = "\t";
	private static class RcFileMapper extends Mapper<Object, BytesRefArrayWritable, Text, Text> {
		
		private static String getValue(BytesRefArrayWritable value, int index) {
			Text txt = new Text();
			BytesRefWritable val = value.get(index);
			try {
				txt.set(val.getData(), val.getStart(), val.getLength());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return txt.toString();
		}
		
		protected void map(Object key, BytesRefArrayWritable value, Context context)
				throws IOException, InterruptedException {
			InputSplit inputSplit = (InputSplit)context.getInputSplit();
            String pathName = ((FileSplit)inputSplit).getPath().toString();
            if (!pathName.contains("music_mds_lighttpd_main_day")) return;
            String baiduid = getValue(value, 14);
            if (baiduid == null || !baiduid.matches("[0-9a-zA-Z]+")) return;
            //  /music/warehouse/music_mds_lighttpd_main_day/event_day=20160905/event_terminal_type=pcweb/000000_0
            String[] paths = pathName.split("\\/");
            if (paths.length < 8) return;
            String date = paths[6].split("=")[1];
            String device_type = paths[7].split("=")[1];
            
            //0,2,7,10,14,18
            //product,hour,domain,refer_domain,baiduid,device_type
			String product = getValue(value, 0);
			String hour = getValue(value, 2);
			String refer_domain = getValue(value, 10);
			
			context.write(new Text(baiduid), new Text(date + "-" + hour + TAB + device_type + TAB + product  + TAB + refer_domain));
		}
	}

	private static class RcFileReduce extends Reducer<Text, Text, Text, Text> {
		
		public static String createJsonString(Object object){  
	        String jsonString = "";  
	        try {  
	            jsonString = JSON.toJSONString(object);  
	        } catch (Exception e) {  
	            // TODO: handle exception  
	        }  
	          
	        return jsonString;  
	    }
		
		protected void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			HashMap<String, Integer> hMap = new HashMap<String, Integer>();
			for (Text val : values) {
				if (hMap.containsKey(val.toString())) {
					int cnt = hMap.get(val.toString()) + 1;
					hMap.put(val.toString(), cnt);
				} else {
					hMap.put(val.toString(), 1);
				}
			}
			context.write(key, new Text(createJsonString(hMap)));
		}
	}

	public static boolean runLoadMapReducue(Configuration conf, Path input, Path output) 
			throws IOException, ClassNotFoundException, InterruptedException {
		conf.set("hive.io.file.read.all.columns", "false");
		conf.set("hive.io.file.readcolumn.ids", "0,2,10,14,18");
		Job job = Job.getInstance(conf);
		job.setJarByClass(UserBrowseRecord.class);
		job.setJobName("Evan_UserBrowseRecord");
		job.setNumReduceTasks(1000);
		job.setMapperClass(RcFileMapper.class);
		job.setReducerClass(RcFileReduce.class);
		job.setInputFormatClass(RCFileMapReduceInputFormat.class);
		RCFileMapReduceInputFormat.addInputPath(job, input);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, output);
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		return job.waitForCompletion(true);
	}

	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		//conf.set("mapreduce.job.queuename", "mapred");
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
		String out = "/user/work/evan/tmp/UserBrowseRecord";
		Path path = new Path(out);
		hdfs.delete(path, true);
		  
		UserBrowseRecord.runLoadMapReducue(conf, new Path(args[0]), new Path(out));
	}
}