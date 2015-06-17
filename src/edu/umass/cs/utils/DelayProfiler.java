package edu.umass.cs.utils;

import java.util.HashMap;

import edu.umass.cs.gns.util.Util;

/**
 * @author V. Arun
 */
public class DelayProfiler {
	private static HashMap<String, Double> delays = new HashMap<String, Double>();

	/**
	 * @param field
	 * @return As specified by {@link HashMap#put(Object, Object)}/
	 */
	public synchronized static boolean register(String field) {
		if (delays.containsKey(field))
			return false;
		delays.put(field, 0.0);
		return true;
	}

	/**
	 * @param field
	 * @param time
	 */
	public synchronized static void update(String field, double time) {
		register(field); // register if not registered
		double delay = delays.get(field);
		delay = Util.movingAverage(System.currentTimeMillis() - time, delay);
		delays.put(field, delay);
	}

	/**
	 * @param field
	 * @param time
	 * @param n
	 */
	public synchronized static void update(String field, long time, int n) {
		for (int i = 0; i < n; i++)
			update(field,
					System.currentTimeMillis()
							- (System.currentTimeMillis() - time) * 1.0 / n);
	}

	/**
	 * @param field
	 * @return The delay.
	 */
	public synchronized static double get(String field) {
		return delays.containsKey(field) ? delays.get(field) : 0.0;
	}

	/**
	 * @return Statistics as a string.
	 */
	public synchronized static String getStats() {
		String s = "[ ";
		int count = 0;
		if (!delays.isEmpty())
			for (String field : delays.keySet()) {
				s += ((count++ > 0 ? " | " : "") + field + ":" + Util.mu(delays
						.get(field)));
			}
		return s + " ]";
	}
}