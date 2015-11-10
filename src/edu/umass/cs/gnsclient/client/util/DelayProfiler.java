/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsclient.client.util;

import java.util.HashMap;

import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 */
public class DelayProfiler {
	private static HashMap<String, Double> averageMillis = new HashMap<String, Double>();
	private static HashMap<String, Double> averageNanos = new HashMap<String, Double>();
	private static HashMap<String, Double> averages = new HashMap<String, Double>();
	private static HashMap<String, Double> stdDevs = new HashMap<String, Double>();
	private static HashMap<String, Double> counters = new HashMap<String, Double>();

	/**
	 * @param field
	 * @param map
	 */
	public static void register(String field, HashMap<String, Double> map) {
		synchronized (map) {
			if (map.containsKey(field))
				return;
			map.put(field, 0.0);
		}
		synchronized (stdDevs) {
			stdDevs.put(field, 0.0);
		}
	}

	/**
	 * @param field
	 * @param time
	 */
	public static void updateDelay(String field, double time) {
		double delay;
		long endTime = System.currentTimeMillis();
		synchronized (averageMillis) {
			register(field, averageMillis); // register if not registered
			delay = averageMillis.get(field);
			delay = Util.movingAverage(endTime - time, delay);
			averageMillis.put(field, delay);
		}
		synchronized (stdDevs) {
			// update deviation
			double dev = stdDevs.get(field);
			dev = Util.movingAverage(endTime - time - delay, dev);
			stdDevs.put(field, dev);
		}
	}

	/**
	 * @param field
	 * @param time
	 */
	public static void updateDelayNano(String field, double time) {
		double delay;
		long endTime = System.nanoTime();
		synchronized (averageNanos) {
			register(field, averageNanos); // register if not registered
			delay = averageNanos.get(field);
			delay = Util.movingAverage(endTime - time, delay);
			averageNanos.put(field, delay);
		}
		synchronized (stdDevs) {
			// update deviation
			double dev = stdDevs.get(field);
			dev = Util.movingAverage(endTime - time - delay, dev);
			stdDevs.put(field, dev);
		}
	}

	/**
	 * @param field
	 * @param time
	 * @param n
	 */
	public static void updateDelay(String field, long time, int n) {
		for (int i = 0; i < n; i++)
			updateDelay(field,
					System.currentTimeMillis()
							- (System.currentTimeMillis() - time) * 1.0 / n);
	}

	/**
	 * @param field
	 * @return The delay.
	 */
	public static double get(String field) {
		synchronized (averageMillis) {
			return averageMillis.containsKey(field) ? averageMillis.get(field)
					: 0.0;
		}
	}

	/**
	 * @param field
	 * @param sample
	 */
	public static void updateMovAvg(String field, double sample) {
		double value;
		synchronized (averages) {
			register(field, averages); // register if not registered
			// update value
			value = averages.get(field);
			value = Util.movingAverage(sample, value);
			averages.put(field, value);
		}
		synchronized (stdDevs) {
			// update deviation
			double dev = stdDevs.get(field);
			dev = Util.movingAverage(sample - value, dev);
			stdDevs.put(field, dev);
		}
	}

	/**
	 * @param field
	 * @param incr
	 */
	public static void updateCount(String field, int incr) {
		synchronized (counters) {
			register(field, counters);
			double value = counters.get(field);
			value += incr;
			counters.put(field, value);
		}
	}

	/**
	 * @return Statistics as a string.
	 */
	public static String getStats() {
		String s = "[ ";
		s += statsHelper(averageMillis, "ms");
		s += statsHelper(averageNanos, "ns");
		s += statsHelper(averages, "");
		s += statsHelper(counters, "");
		return (s + "]").replace(" | ]", " ]");
	}

	private static String statsHelper(HashMap<String, Double> map, String units) {
		String s = "";
		synchronized(map) {
			for (String field : map.keySet()) {
				s += (field + ":" + Util.df(map.get(field)) + "/"
						+ (stdDevs.get(field) > 0 ? "+" : "")
						+ Util.df(stdDevs.get(field)) + units + " | ");
			}
		}
		return s;
	}
}