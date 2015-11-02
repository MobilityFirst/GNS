package edu.umass.cs.utils;

import java.util.HashMap;

/**
 * @author V. Arun
 */
public class DelayProfiler {
	private static HashMap<String, Double> averageMillis = new HashMap<String, Double>();
	private static HashMap<String, Double> averageNanos = new HashMap<String, Double>();
	private static HashMap<String, Double> averages = new HashMap<String, Double>();
	private static HashMap<String, Double> stdDevs = new HashMap<String, Double>();
	private static HashMap<String, Double> counters = new HashMap<String, Double>();
	private static HashMap<String, Double> instarates = new HashMap<String, Double>();

	private static HashMap<String, Double> lastArrivalNanos = new HashMap<String, Double>();

	private static HashMap<String, Double> lastRecordedNanos = new HashMap<String, Double>();
	private static HashMap<String, Double> lastCount = new HashMap<String, Double>();

	/**
	 * @param field
	 * @param map
	 */
	public static void register(String field, HashMap<String, Double> map) {
		synchronized (map) {
			if (map.containsKey(field))
				return;
			if(map == lastRecordedNanos) map.put(field, (double)System.nanoTime());
			else map.put(field, 0.0);
		}
		synchronized (stdDevs) {
			stdDevs.put(field, 0.0);
		}
	}

	/**
	 * @param field
	 * @param time
	 * @param alpha 
	 */
	public static void updateDelay(String field, double time, double alpha) {
		double delay;
		long endTime = System.currentTimeMillis();
		synchronized (averageMillis) {
			register(field, averageMillis); // register if not registered
			delay = averageMillis.get(field);
			delay = Util.movingAverage(endTime - time, delay, alpha);
			averageMillis.put(field, delay);
		}
		synchronized (stdDevs) {
			// update deviation
			double dev = stdDevs.get(field);
			dev = Util.movingAverage(endTime - time - delay, dev, alpha);
			stdDevs.put(field, dev);
		}
	}
	/**
	 * @param field
	 * @param time
	 */
	public static void updateDelay(String field, double time) {
		updateDelay(field, time, Util.ALPHA);
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
	public static void updateDelayNano(String field, long time, int n) {
		for (int i = 0; i < n; i++)
			updateDelayNano(field,
					System.nanoTime()
							- (System.nanoTime() - time) * 1.0 / n);
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
					: averageNanos.containsKey(field) ? averageNanos.get(field) :
						averages.containsKey(field) ? averages.get(field) :
							counters.containsKey(field) ? counters.get(field) :
								instarates.containsKey(field) ? instarates.get(field) :
									0.0;
		}
	}

	/**
	 * @param field
	 * @param sample
	 */
	public static void updateMovAvg(String field, double sample) {
		updateMovAvg(field, sample, Util.ALPHA);
	}

	/**
	 * @param field
	 * @param sample
	 * @param alpha
	 */
	public static void updateMovAvg(String field, double sample, double alpha) {
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
	 * @param field
	 * @param value
	 */
	public static void updateValue(String field, double value) {
		synchronized (counters) {
			register(field, counters);
			counters.put(field, value);
		}
	}

	/**
	 * @param field
	 * @param numArrivals
	 * @param samplingFactor 
	 */
	public static void updateInterArrivalTime(String field, int numArrivals, int samplingFactor) {
		updateInterArrivalTime(field, numArrivals, samplingFactor, Util.ALPHA);
	}
	/**
	 * @param field
	 * @param numArrivals
	 * @param samplingFactor 
	 * @param alpha 
	 */
	public static void updateInterArrivalTime(String field, int numArrivals, int samplingFactor, double alpha) {
		if(!Util.oneIn(samplingFactor)) return;
		synchronized (lastArrivalNanos) {
			register(field, lastArrivalNanos);
			long curTime = System.nanoTime();
			double value = lastArrivalNanos.containsKey(field) ? lastArrivalNanos
					.get(field) : curTime;
			if (value == 0)
				value = curTime;
			DelayProfiler.updateMovAvg(field, (curTime - (long) value)
					/ (numArrivals*samplingFactor));
			lastArrivalNanos.put(field, System.nanoTime() * 1.0);
		}
	}
	/**
	 * @param field
	 * @param numArrivals
	 */
	public static void updateInterArrivalTime(String field, int numArrivals) {
		updateInterArrivalTime(field, numArrivals, 1);
	}
	
	/**
	 * @param field
	 * @param numArrivals
	 * @param samplingFactor 
	 */
	public static void updateRate(String field, int numArrivals, int samplingFactor) {
		if(!Util.oneIn(samplingFactor)) return;
		register(field, lastCount);
		register(field, lastRecordedNanos);
		register(field, instarates);
		synchronized (lastCount) {
			double count = lastCount.get(field) + samplingFactor;
			if (count == numArrivals) {
				instarates.put(field, numArrivals*1000*1000*1000.0
						/ (System.nanoTime() - lastRecordedNanos.get(field)));
				lastCount.put(field, (double) 0);
				lastRecordedNanos.put(field, (double) System.nanoTime());
			} else
				lastCount.put(field, count);
		}
	}
	
	/**
	 * @param field
	 * @param numArrivals
	 */
	public static void updateRate(String field, int numArrivals) {
		updateRate(field, numArrivals, 1);
	}

	/**
	 * @param field
	 * @return Throughput calculated from interarrival time.
	 */
	public static double getThroughput(String field) {
		return averages.containsKey(field) && averages.get(field) > 0 ? 1000 * 1000 * 1000.0 / (averages.get(field)) : 0;
	}
	/**
	 * @param field
	 * @return Moving average of instantaneous rate.
	 */
	public static double getRate(String field) {
		return instarates.containsKey(field) ? (instarates.get(field)) : 0;
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
		s += statsHelper(instarates, "/s");

		return (s + "]").replace(" | ]", " ]");
	}

	private static String statsHelper(HashMap<String, Double> map, String units) {
		String s = "";
		synchronized (map) {
			for (String field : map.keySet()) {
				boolean rateParam = lastArrivalNanos.containsKey(field);
				s += (field
						+ ":"
						+ (!rateParam ? Util.df(map.get(field)) : Util
								.df(getThroughput(field)))
						+ "/"
						+ (stdDevs.get(field) > 0 ? "+" : "")
						+ (!rateParam ? Util.df(stdDevs.get(field)) : Util
								.df(1000 * 1000 * 1000.0 / stdDevs.get(field)))
						+ (!rateParam ? units : "/s") + " | ");
			}
		}
		return s;
	}
}