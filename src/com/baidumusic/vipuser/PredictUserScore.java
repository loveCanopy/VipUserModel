package com.baidumusic.vipuser;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import de.bwaldvogel.liblinear.Feature;
import de.bwaldvogel.liblinear.FeatureNode;
import de.bwaldvogel.liblinear.Linear;
import de.bwaldvogel.liblinear.Model;

public class PredictUserScore {
    private static DecimalFormat df = new java.text.DecimalFormat("0.000000");
    public static class PredictMap extends Mapper<LongWritable, Text, Text, Text> {
    	private static boolean flag_predict_probability = true;
    	private static final Pattern COLON = Pattern.compile(":");
    	private static final String SPACE = " ";
    	
    	private static double atof(String s) {
    		return Double.valueOf(s).doubleValue();
    	}

    	private static int atoi(String s) {
    		return Integer.parseInt(s);
    	}
        private static Model vip_model;
        private static Model shoumai_model;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
        	super.setup(context);
            try {
            	File vip = new File("vip.model");
            	vip_model = Model.load(vip);
            	
            	File shoumai = new File("shoumai.model");
            	shoumai_model = Model.load(shoumai);
            } catch (IOException e) {
                System.err.println("Exception reading DistributedCache: " + e);
            }
        }

        public void map (LongWritable key, Text value, Context context)
        		throws IOException, InterruptedException {
        	/*
        	 00006049018761CC849E1A6482AB3552 1 1 1:1.000000 2:1.000000 3:1.000000 4:1.000000 5:1.000000 6:1.000000 8:1.000000 
        	 */
        	String line = value.toString();
        	if (line == null) return;
        	String[] lparts = line.split(SPACE);
        	if (lparts.length < 4) return;
        	String baiduid = lparts[0];
        	String vip_label = lparts[1];
        	String shoumai_label = lparts[2];

        	int idx = line.indexOf(SPACE);
        	String subline = line.substring(idx + 5);
        	String vip_result = ModelPredict(vip_model, subline);
        	String shoumai_result = ModelPredict(shoumai_model, subline);
        	String result = vip_label + "\t" + (vip_result.length() > 0 ? vip_result : "0\t0\t0\t0") + "; "
        			+ shoumai_label + "\t" + (shoumai_result.length() > 0 ? shoumai_result : "0\t0\t0\t0");
        	context.write(new Text(baiduid), new Text(result));
        }

        public static String ModelPredict(Model model, String line) throws IOException {
    		int nr_class = model.getNrClass();
    		double[] prob_estimates = null;
    		int n;
    		int nr_feature = model.getNrFeature();
    		if (model.getBias() >= 0) {
    			n = nr_feature + 1;
    		} else {
    			n = nr_feature;
    		}

    		if (flag_predict_probability && !model.isProbabilityModel()) {
    			throw new IllegalArgumentException("probability output is only supported for logistic regression");
    		}

    		if (flag_predict_probability) {
    			prob_estimates = new double[nr_class];
    		}

    		List<Feature> x = new ArrayList<Feature>();
    		StringTokenizer st = new StringTokenizer(line, " \t\n");

    		while (st.hasMoreTokens()) {
    			String[] split = COLON.split(st.nextToken(), 2);
    			if (split == null || split.length < 2) {
    				throw new RuntimeException("Wrong input format at line " + line);
    			}

    			try {
    				int idx = atoi(split[0]);
    				double val = atof(split[1]);

    				// feature indices larger than those in training are not used
    				if (idx <= nr_feature) {
    					Feature node = new FeatureNode(idx, val);
    					x.add(node);
    				}
    			} catch (NumberFormatException e) {
    				throw new RuntimeException("Wrong input format at line " + line, e);
    			}
    		}

    		if (model.getBias() >= 0) {
    			Feature node = new FeatureNode(n, model.getBias());
    			x.add(node);
    		}

    		Feature[] nodes = new Feature[x.size()];
    		nodes = x.toArray(nodes);

    		Map<Integer, String> map = new TreeMap<Integer, String>(new Comparator<Integer>() {
    			public int compare(Integer obj1, Integer obj2) {
    				return obj1 - obj2;
    			}
    		});
    		double predict_label = 0;
    		String res = "";
    		if (flag_predict_probability) {
    			int[] labels = model.getLabels();
    			assert prob_estimates != null;
    			predict_label = Linear.predictProbability(model, nodes, prob_estimates);

    			for (int j = 0; j < model.getNrClass(); j++) {
    				map.put(labels[j], df.format(prob_estimates[j]));
    			}

    			for (String value : map.values()) {
    				res += "\t" + value;
    			}
    		} else {
    			predict_label = Linear.predict(model, nodes);
    		}
    		res = predict_label + (res.length() == 0 ? "" : (res));
    		return res;
    	}
    }
    
    public static void main(String[] args) throws IOException, 
        ClassNotFoundException, InterruptedException, URISyntaxException 
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length == 0) {
        	System.err.println("Usage: PredictUserScore <in>");
            System.exit(-1);
        }
        
        String queue = "mapred";
		if (args.length > 1) {
			queue = args[1].matches("hql|dstream|mapred|udw|user|common") ? args[1] : "mapred";
		}
		conf.set("mapreduce.job.queuename", queue);
		
        Job job = Job.getInstance(conf, "Evan_PredictUserScore");
        job.setJarByClass(PredictUserScore.class);
        job.setMapperClass(PredictMap.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.addCacheFile(new URI("/user/work/evan/model/vip.model#vip.model"));
        job.addCacheFile(new URI("/user/work/evan/model/shoumai.model#shoumai.model"));
        FileInputFormat.setInputPaths(job, otherArgs[0]);
        Path outPath = new Path("/user/work/evan/output/PredictUserScore");
        FileSystem hdfs = FileSystem.get(conf);
        hdfs.delete(outPath, true);
        FileOutputFormat.setOutputPath(job, outPath);
        job.setNumReduceTasks(0);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
