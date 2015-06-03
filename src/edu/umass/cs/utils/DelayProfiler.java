package edu.umass.cs.utils;

import java.util.HashMap;

import edu.umass.cs.gns.util.Util;

/**
 * @author V. Arun
 */
public class DelayProfiler {
	private static HashMap<String, Double> delays = new HashMap<String, Double>();

	public synchronized static boolean register(String field) {
		if (delays.containsKey(field))
			return false;
		delays.put(field, 0.0);
		return true;
	}

	public synchronized static void update(String field, double time) {
		register(field); // register if not registered
		double delay = delays.get(field);
		delay = Util.movingAverage(System.currentTimeMillis() - time, delay);
		delays.put(field, delay);
	}

	public synchronized static void update(String field, long time, int n) {
		for (int i = 0; i < n; i++)
			update(field,
					System.currentTimeMillis()
							- (System.currentTimeMillis() - time) * 1.0 / n);
	}

	public synchronized static double get(String field) {
		return delays.containsKey(field) ? delays.get(field) : 0.0;
	}

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