package com.baidumusic.useranaly;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

public class FansOfSingerAddPortrait {
	private final static String TAB = "\t";
	
	private static final ArrayList<String> ItemList = new ArrayList<String>();
	static {
		ItemList.add("性别");
		ItemList.add("年龄");
		ItemList.add("星座");
		ItemList.add("消费水平");
		ItemList.add("教育水平");
		ItemList.add("所在行业");
		ItemList.add("资产状况");
		ItemList.add("人生阶段");
	}
	
	private static final ArrayList<String> hobbyList = new ArrayList<String>();
	static {
		hobbyList.add("个护美容");
		hobbyList.add("书籍阅读");
		hobbyList.add("休闲爱好");
		hobbyList.add("体育健身");
		hobbyList.add("公益");
		hobbyList.add("医疗健康");
		hobbyList.add("商务服务");
		hobbyList.add("婚恋交友");
		hobbyList.add("家电数码");
		hobbyList.add("对外开放");
		hobbyList.add("建材家居");
		hobbyList.add("影视音乐");
		hobbyList.add("房产");
		hobbyList.add("教育培训");
		hobbyList.add("旅游酒店");
		hobbyList.add("星座运势");
		hobbyList.add("服饰鞋包");
		hobbyList.add("母婴亲子");
		hobbyList.add("求职创业");
		hobbyList.add("汽车");
		hobbyList.add("游戏");
		hobbyList.add("生活服务");
		hobbyList.add("网络购物");
		hobbyList.add("花鸟萌宠");
		hobbyList.add("资讯");
		hobbyList.add("软件应用");
		hobbyList.add("金融财经");
		hobbyList.add("非汽车类机动车");
		hobbyList.add("餐饮美食");
	}
	
	private static final ArrayList<String> aList = new ArrayList<String>();
	static {
		aList.add("动漫");
		aList.add("广播电台");
		aList.add("影音在线付费意愿中");
		aList.add("影音在线付费意愿低");
		aList.add("影音在线付费意愿高");
		aList.add("微电影");
		aList.add("戏曲曲艺");
		aList.add("演出票务");
		aList.add("电影");
		aList.add("电视剧");
		aList.add("纪录片");
		aList.add("综艺");
		aList.add("音乐");
	}
	
	private static String getPortraitInfo(String line) {
		HashSet<String> hSet = new HashSet<String>();
		HashSet<String> reSet = new HashSet<String>();
		HashMap<String, String> hMap = new HashMap<String, String>();
		String[] lpart = line.replaceAll(" ", "").split(",");
		for (String item : lpart) {
			String[] ipart = item.split("/|\\|");
			if (ipart.length >= 3) {
				if (hobbyList.contains(ipart[0])) {
					hSet.add(ipart[0]);
					if (ipart[0].equals("影视音乐") && aList.contains(ipart[1])) {
						reSet.add(ipart[1]);
					}
				} else if (ItemList.contains(ipart[0])) {
					hMap.put(ipart[0], ipart[1]);
				}
			}
		}
		StringBuilder basic = new StringBuilder();
		int cnt = 0;
		for (String item : ItemList) {
			String val = hMap.containsKey(item) ? hMap.get(item) : "NAN";
			if (cnt++ == 0) {
				basic.append(val);
			} else {
				basic.append(TAB).append(val);
			}
		}
		
		StringBuilder hobby = new StringBuilder();
		int cnt2 = 0;
		for (String item : hobbyList) {
			String val = hSet.contains(item) ? "1" : "0";
			if (cnt2++ == 0) {
				hobby.append(val);
			} else {
				hobby.append(TAB).append(val);
			}
		}
		
		int cnt3 = 0;
		StringBuilder hobby_2level = new StringBuilder();
		for (String item : aList) {
			String val = reSet.contains(item) ? "1" : "0";
			if (cnt3++ == 0) {
				hobby_2level.append(val);
			} else {
				hobby_2level.append(TAB).append(val);
			}
		}
		return basic.toString() + TAB + hobby.toString() + TAB + hobby_2level.toString();
	}
	
	private static HashMap<Integer, String> numMap = new HashMap<Integer, String>();
	static {
		numMap.put(1,"个护美容");
		numMap.put(2,"书籍阅读");
		numMap.put(3,"休闲爱好");
		numMap.put(4,"体育健身");
		numMap.put(5,"公益");
		numMap.put(6,"医疗健康");
		numMap.put(7,"商务服务");
		numMap.put(8,"婚恋交友");
		numMap.put(9,"家电数码");
		numMap.put(10, "对外开放");
		numMap.put(11, "建材家居");
		numMap.put(12, "影视音乐");
		numMap.put(13, "房产");
		numMap.put(14, "教育培训");
		numMap.put(15, "旅游酒店");
		numMap.put(16, "星座运势");
		numMap.put(17, "服饰鞋包");
		numMap.put(18, "母婴亲子");
		numMap.put(19, "求职创业");
		numMap.put(20, "汽车");
		numMap.put(21, "游戏");
		numMap.put(22, "生活服务");
		numMap.put(23, "网络购物");
		numMap.put(24, "花鸟萌宠");
		numMap.put(25, "资讯");
		numMap.put(26, "软件应用");
		numMap.put(27, "金融财经");
		numMap.put(28, "非汽车类机动车");
		numMap.put(29, "餐饮美食");
		numMap.put(30, "影视音乐/动漫");
		numMap.put(31, "影视音乐/广播电台");
		numMap.put(32, "影视音乐/影音在线付费意愿中");
		numMap.put(33, "影视音乐/影音在线付费意愿低");
		numMap.put(34, "影视音乐/影音在线付费意愿高");
		numMap.put(35, "影视音乐/微电影");
		numMap.put(36, "影视音乐/戏曲曲艺");
		numMap.put(37, "影视音乐/演出票务");
		numMap.put(38, "影视音乐/电影");
		numMap.put(39, "影视音乐/电视剧");
		numMap.put(40, "影视音乐/纪录片");
		numMap.put(41, "影视音乐/综艺");
		numMap.put(42, "影视音乐/音乐");
	}
	
	private static class FansMapper extends Mapper<LongWritable, Text, Text, Text> {
		private static HashMap<String, HashSet<String>> uMap = new HashMap<String, HashSet<String>>();
		private static HashSet<String> lSet = new HashSet<String>();
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			File fans = new File("fansofsinger");
			BufferedReader br = new BufferedReader(new FileReader(fans));
			String line = "";
			while ((line = br.readLine()) != null) {
				String[] lpart = line.split(TAB);
				if (5 != lpart.length) continue;
				if (!uMap.containsKey(lpart[1])) {
					HashSet<String> tmpSet = new HashSet<String>();
					tmpSet.add(line);
					uMap.put(lpart[1], tmpSet);
				} else {
					HashSet<String> tmpSet = uMap.get(lpart[1]);
					tmpSet.add(line);
					uMap.put(lpart[1], tmpSet);
				}
			}
			br.close();
			System.out.println("uMap.size() = " + uMap.size());
		}

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] lparts = line.split("\001");
			if (2 == lparts.length) {
				if (uMap.containsKey(lparts[0])) {
					String portrait = getPortraitInfo(lparts[1]);
					lSet.add(lparts[0]);
					HashSet<String> tmpSet = uMap.get(lparts[0]);
					for (String val : tmpSet) {
						context.write(new Text(val), new Text(portrait));
					}
				}
			}
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Entry<String, HashSet<String>> entry : uMap.entrySet()) {
				String key = entry.getKey();
				HashSet<String> tmpSet = entry.getValue();
				if (!lSet.contains(key)) {
					for (String val : tmpSet) {
						context.write(new Text(val), new Text());
					}
				}
			}
		}
	}

	private static class FansReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			String portrait = "NAN\tNAN\tNAN\tNAN\tNAN\tNAN\tNAN\tNAN\t0\t0\t0\t0\t0\t0\t0\t0"
					+ "\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0"
					+ "\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0";
			for (Text val : values) {
				if (val.toString() != null && val.toString().length() > 0) {
					portrait = val.toString();
				}
			}
			context.write(key, new Text(portrait));
		}
	}
	
	private static class GroupMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//1030	00086FB9C70D75D3E20A45497EADB7AB	10	{纯音乐=1}	{流行=1}	NAN	NAN	NAN...
			String line = value.toString();
			String[] lparts = line.split(TAB);
			if (55 == lparts.length) {
				String singer_id = lparts[0];
				context.write(new Text(singer_id), new Text(line));
			}
		}
	}
	
	private static class GroupReduce extends Reducer<Text, Text, Text, Text> {
		private static HashMap<String, Integer> AddMap(HashMap<String, Integer> map, String key) {
			if (map.containsKey(key)) {
				int cnt = map.get(key);
				map.put(key, cnt + 1);
			} else {
				map.put(key, 1);
			}
			return map;
		}
		
		private static HashMap<String, Integer> MapAddMap(HashMap<String, Integer> map, String line) {
			line = line.replaceAll("\\{|\\}", "");
			String[] lpart = line.split(",");
			for (String l : lpart) {
				String key = l.split("=")[0];
				String val = l.split("=")[1];
				if (map.containsKey(key)) {
					int cnt = map.get(key);
					map.put(key, cnt + Integer.valueOf(val));
				} else {
					map.put(key, Integer.valueOf(val));
				}
			}
			return map;
		}
		
		protected void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			HashMap<String, Integer> langMap = new HashMap<String, Integer>();
			HashMap<String, Integer> styleMap = new HashMap<String, Integer>();
			HashMap<String, Integer> sexMap = new HashMap<String, Integer>();
			HashMap<String, Integer> ageMap = new HashMap<String, Integer>();
			HashMap<String, Integer> starMap = new HashMap<String, Integer>();
			HashMap<String, Integer> buyMap = new HashMap<String, Integer>();
			HashMap<String, Integer> eduMap = new HashMap<String, Integer>();
			HashMap<String, Integer> proMap = new HashMap<String, Integer>();
			HashMap<String, Integer> worthMap = new HashMap<String, Integer>();
			HashMap<String, Integer> stageMap = new HashMap<String, Integer>();
			HashMap<String, Integer> hobbyMap = new HashMap<String, Integer>();
			int dist = 0;
			int count = 0;
			//{英语=1,国语=5}	{unknown=2,流行=4}
			for (Text val : values) {
				String[] lparts = val.toString().split(TAB);
				if (55 == lparts.length) {
					dist += 1;
					count += Integer.valueOf(lparts[2]);
					String language = lparts[3];
					langMap = MapAddMap(langMap, language);
					
					String style = lparts[4];
					styleMap = MapAddMap(styleMap, style);
					
					String sex = lparts[5];
					sexMap = AddMap(sexMap, sex);
					
					String age = lparts[6];
					ageMap = AddMap(ageMap, age);
					
					String star = lparts[7];
					starMap = AddMap(starMap, star);
					
					String buy = lparts[8];
					buyMap = AddMap(buyMap, buy);
					
					String edu = lparts[9];
					eduMap = AddMap(eduMap, edu);
					
					String pro = lparts[10];
					proMap = AddMap(proMap, pro);
					
					String worth = lparts[11];
					worthMap = AddMap(worthMap, worth);
					
					String stage = lparts[12];
					stageMap = AddMap(stageMap, stage);
					
					for (int i=13; i<55; i++) {
						String tmp = lparts[i];
						if (tmp.equals("1")) {
							String item = numMap.get(i - 12);
							hobbyMap = AddMap(hobbyMap, item);
						}
					}
				}
			}
			context.write(key, new Text(dist + TAB + count
					 + TAB +langMap.toString()
					+ TAB + styleMap.toString()
					+ TAB + sexMap.toString()
					+ TAB + ageMap.toString()
					+ TAB + starMap.toString()
					+ TAB + buyMap.toString()
					+ TAB + eduMap.toString()
					+ TAB + proMap.toString()
					+ TAB + worthMap.toString()
					+ TAB + stageMap.toString()
					+ TAB + hobbyMap.toString()));
		}
	}
	
	public static boolean runLoadMapReducue(Configuration conf, String input) 
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		FileSystem hdfs = FileSystem.get(conf);
		String output1 = "/user/work/evan/tmp/FansOfSingerAddPortrait1";
		String output2 = "/user/work/evan/tmp/FansOfSingerAddPortrait2";
		hdfs.delete(new Path(output1), true);
		hdfs.delete(new Path(output2), true);
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(FansOfSingerAddPortrait.class);
		job.setJobName("Evan_FansOfSingerAddPortrait-L1");
		job.setNumReduceTasks(10);
		job.setMapperClass(FansMapper.class);
		job.setReducerClass(FansReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.addCacheFile(new URI("/user/work/evan/tmp/StatFansOfSinger/part-r-00000#fansofsinger"));
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, new Path(output1));
		job.waitForCompletion(true);
		
		job = Job.getInstance(conf);
		job.setJarByClass(FansOfSingerAddPortrait.class);
		job.setJobName("Evan_FansOfSingerAddProtrait-L2");
		job.setNumReduceTasks(1);
		job.setMapperClass(GroupMapper.class);
		job.setReducerClass(GroupReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, output1);
		FileOutputFormat.setOutputPath(job, new Path(output2));
		return job.waitForCompletion(true);
	}

	public static void main(String[] args) throws IOException, 
			ClassNotFoundException, InterruptedException, URISyntaxException {
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
		
		FansOfSingerAddPortrait.runLoadMapReducue(conf, args[0]);
	}
}