package com.baidumusic.vipuser;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class MathStatics {
	private static final DecimalFormat df = new DecimalFormat("0.000000");
	private static final String TAB = "\t";

	public static void main(String[] args) {
		Integer[] testData = new Integer[] { 3, 3, 9, 5 };
		System.out.println("最大值：" + getMax(testData));
		System.out.println("最小值：" + getMin(testData));
		System.out.println("计数：" + getCount(testData));
		System.out.println("求和：" + getSum(testData));
		System.out.println("求平均：" + getAverage(testData));
		System.out.println("方差：" + getVariance(testData));
		System.out.println("标准差：" + getStandardDiviation(testData));
		System.out.println("中位数： " + getMedian(testData));
		// testData=new int[]{2,3,3,1,1,2};
		System.out.println("众数：" + getMaxValMode(testData));
		System.out.println(CombineStat(testData));
	}

	/**
	 * 求给定双精度数组中值的最大值
	 * 
	 * @param inputData
	 *            输入数据数组
	 * @return 运算结果,如果输入值不合法，返回为-1
	 */
	public static int getMax(Integer[] inputData) {
		if (inputData == null || inputData.length == 0)
			return -1;
		int len = inputData.length;
		int max = inputData[0];
		for (int i = 0; i < len; i++) {
			if (max < inputData[i])
				max = inputData[i];
		}
		return max;
	}

	/**
	 * 求求给定双精度数组中值的最小值
	 * 
	 * @param inputData
	 *            输入数据数组
	 * @return 运算结果,如果输入值不合法，返回为-1
	 */
	public static int getMin(Integer[] inputData) {
		if (inputData == null || inputData.length == 0)
			return -1;
		int len = inputData.length;
		int min = inputData[0];
		for (int i = 0; i < len; i++) {
			if (min > inputData[i])
				min = inputData[i];
		}
		return min;
	}

	/**
	 * 求给定双精度数组中值的和
	 * 
	 * @param inputData
	 *            输入数据数组
	 * @return 运算结果
	 */
	public static int getSum(Integer[] inputData) {
		if (inputData == null || inputData.length == 0)
			return -1;
		int len = inputData.length;
		int sum = 0;
		for (int i = 0; i < len; i++) {
			sum = sum + inputData[i];
		}

		return sum;

	}

	/**
	 * 求给定双精度数组中值的数目
	 * 
	 * @param input
	 *            Data 输入数据数组
	 * @return 运算结果
	 */
	public static int getCount(Integer[] inputData) {
		if (inputData == null)
			return -1;

		return inputData.length;
	}

	/**
	 * 求给定双精度数组中值的平均值
	 * 
	 * @param inputData
	 *            输入数据数组
	 * @return 运算结果
	 */
	public static double getAverage(Integer[] inputData) {
		if (inputData == null || inputData.length == 0)
			return -1;
		int len = inputData.length;
		double result;
		result = getSum(inputData) * 1.0 / len;

		return Double.valueOf(df.format(result));
	}

	/**
	 * 求给定双精度数组中值的平方和
	 * 
	 * @param inputData
	 *            输入数据数组
	 * @return 运算结果
	 */
	public static double getSquareSum(Integer[] inputData) {
		if (inputData == null || inputData.length == 0)
			return -1;
		int len = inputData.length;
		double sqrsum = 0.0;
		for (int i = 0; i < len; i++) {
			sqrsum = sqrsum + inputData[i] * inputData[i];
		}

		return sqrsum;
	}

	/**
	 * 求给定双精度数组中值的方差
	 * 
	 * @param inputData
	 *            输入数据数组
	 * @return 运算结果
	 */
	public static double getVariance(Integer[] inputData) {
		int count = getCount(inputData);
		double sqrsum = getSquareSum(inputData);
		double average = getAverage(inputData);
		double result;
		result = (sqrsum - count * average * average) / count;

		return result;
	}

	/**
	 * 求给定双精度数组中值的标准差
	 * 
	 * @param inputData
	 *            输入数据数组
	 * @return 运算结果
	 */
	public static double getStandardDiviation(Integer[] inputData) {
		double result;
		// 绝对值化很重要
		result = Math.sqrt(Math.abs(getVariance(inputData)));

		return Double.valueOf(df.format(result));
	}

	/**
	 * 求给定双精度数组中值的中位数
	 * 
	 * @param inputData
	 *            输入数据数组
	 * @return 运算结果
	 */
	public static int getMedian(Integer[] inputData) {
		Arrays.sort(inputData);
		int m = 0;
		int len = inputData.length;
		if (len % 2 == 0) {
			m = (inputData[len / 2] + inputData[len / 2 - 1]) / 2;
		} else {
			m = inputData[(len - 1) / 2];
		}
		return m;
	}

	/**
	 * 此类用于求出数组中权重最大的众数
	 * 
	 * @param inputData
	 *            输入数据数组
	 * @return 运算结果
	 */
	public static int getMaxValMode(Integer[] inputData) {
		Map<Integer, Integer> map = new HashMap<Integer, Integer>();
		for (int i = 0; i < inputData.length; i++) {
			if (map.containsKey(inputData[i])) {
				int number = map.get(inputData[i]) + 1;
				map.put(inputData[i], number);
			} else {
				map.put(inputData[i], 1);
			}
		}

		int mode = -1, val = -1;
		for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
			if (entry.getValue() > val) {
				mode = entry.getKey();
				val = entry.getValue();
			} else if (entry.getValue() == val && entry.getKey() > mode) {
				mode = entry.getKey();
			}
		}

		return mode;
	}

	public static String CombineStat(Integer[] inputData) {
		String result = "0\t0\t0\t0\t0\t0\t0\t0";
		if (inputData == null || inputData.length == 0)
			return result;
		int sum = getSum(inputData);
		int count = getCount(inputData);
		int max = getMax(inputData);
		int min = getMin(inputData);
		double ave = getAverage(inputData);
		int median = getMedian(inputData);
		double std = getStandardDiviation(inputData);
		int mode = getMaxValMode(inputData);
		result = sum + TAB + count + TAB + max + TAB + min + TAB + ave + TAB + median + TAB + std + TAB + mode;
		return result;
	}
}
