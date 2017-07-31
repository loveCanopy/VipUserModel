package com.baidumusic.vipuser;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.regex.Pattern;
import de.bwaldvogel.liblinear.Feature;
import de.bwaldvogel.liblinear.FeatureNode;
import de.bwaldvogel.liblinear.Linear;
import de.bwaldvogel.liblinear.Model;

public class Predictor {

	private static boolean flag_predict_probability = true;
	private static final Pattern COLON = Pattern.compile(":");
	private static final DecimalFormat df = new DecimalFormat("0.000000");

	private static double atof(String s) {
		return Double.valueOf(s).doubleValue();
	}

	private static int atoi(String s) {
		return Integer.parseInt(s);
	}

	public static String doPredict(Model model, String line) throws IOException {
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

	public static void main(String[] argv) throws IOException {
		flag_predict_probability = true;
		String line = "1:5.000000 2:5.000000 3:1.000000 4:1.000000 5:1.000000 6:1.000000 8:1.000000 9:0.357143 10:0.285714 11:0.357143 12:0.800000 13:7.000000 14:2.000000 16:5.000000 17:2.000000 18:3.000000 19:2.000000 20:2.500000 21:2.000000 22:0.500000 23:3.000000 24:5.000000 25:4.000000 26:2.000000 27:1.000000 28:1.250000 29:1.000000 30:0.433013 31:1.000000 36:14.000000 40:5.000000 41:5.000000 42:1.000000 43:1.000000 44:1.000000 45:1.000000 47:1.000000 53:1.000000 54:2.000000 55:2.000000 57:1.000000 61:1.000000 75:4.000000 76:10.000000 97:14.000000 120:14.000000 137:1.000000 159:1.000000 182:1.000000 187:1.000000 190:1.000000 192:1.000000 193:1.000000 194:1.000000 195:1.000000 196:1.000000 197:1.000000 199:1.000000 200:1.000000 201:1.000000 202:1.000000 203:1.000000 204:1.000000 205:1.000000 206:1.000000 208:1.000000";
		String model_path = "C:\\Users\\Administrator\\Desktop\\submodel";
		File logistic = new File(model_path);
		Model model = Model.load(logistic);
		String res = doPredict(model, line);
		System.out.println(res);
	}
}