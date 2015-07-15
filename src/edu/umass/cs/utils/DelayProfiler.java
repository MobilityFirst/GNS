package edu.umass.cs.utils;

import java.util.HashMap;

import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 */
public class DelayProfiler {
	private static HashMap<String, Double> averages = new HashMap<String, Double>();
	private static HashMap<String, Double> stdDevs = new HashMap<String, Double>();

	/**
	 * @param field
	 * @return As specified by {@link HashMap#put(Object, Object)}/
	 */
	public synchronized static boolean register(String field) {
		if (averages.containsKey(field))
			return false;
		averages.put(field, 0.0);
		stdDevs.put(field, 0.0);
		return true;
	}

	/**
	 * @param field
	 * @param time
	 */
	public synchronized static void updateDelay(String field, double time) {
		register(field); // register if not registered
		double delay = averages.get(field);
		delay = Util.movingAverage(System.currentTimeMillis() - time, delay);
		averages.put(field, delay);
		// update deviation
		double dev = stdDevs.get(field);
		dev = Util
				.movingAverage(System.currentTimeMillis() - time - delay, dev);
		stdDevs.put(field, dev);
	}

	/**
	 * @param field
	 * @param time
	 * @param n
	 */
	public synchronized static void updateDelay(String field, long time, int n) {
		for (int i = 0; i < n; i++)
			updateDelay(field,
					System.currentTimeMillis()
							- (System.currentTimeMillis() - time) * 1.0 / n);
	}

	/**
	 * @param field
	 * @return The delay.
	 */
	public synchronized static double get(String field) {
		return averages.containsKey(field) ? averages.get(field) : 0.0;
	}

	/**
	 * @param field
	 * @param sample
	 */
	public synchronized static void updateMovAvg(String field, int sample) {
		register(field); // register if not registered
		// update value
		double value = averages.get(field);
		value = Util.movingAverage(sample, value);
		averages.put(field, value);
		// update deviation
		double dev = stdDevs.get(field);
		dev = Util.movingAverage(sample - value, dev);
		stdDevs.put(field, dev);
	}

	/**
	 * @return Statistics as a string.
	 */
	public synchronized static String getStats() {
		String s = "[ ";
		int count = 0;
		if (!averages.isEmpty())
			for (String field : averages.keySet()) {
				s += ((count++ > 0 ? " | " : "") + field + ":"
						+ Util.df(averages.get(field) * 1000) + "/"
						+ (stdDevs.get(field) > 0 ? "+" : "")
						+ Util.df(stdDevs.get(field) * 1000) + "us");
			}
		return s + " ]";
	}
}