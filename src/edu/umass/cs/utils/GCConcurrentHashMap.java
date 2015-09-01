package edu.umass.cs.utils;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author arun
 * @param <K>
 * @param <V>
 *
 */
public class GCConcurrentHashMap<K, V> extends ConcurrentHashMap<K, V> {

	private static int GC_THRESHOLD_SIZE = 10000;

	private final ConcurrentHashMap<K, Long> putTimes = new ConcurrentHashMap<K, Long>();
	private final TreeSet<TimeKey> putKeys = new TreeSet<TimeKey>();
	private final GCConcurrentHashMapCallback callback;
	private final long gcTimeout;

	class TimeKey implements Comparable<TimeKey> {
		final long time;
		final K key;

		TimeKey(long time, K key) {
			this.time = time;
			this.key = key;
		}

		/*
		 * The spec says compareTo must return 0 iff equals returns true for the
		 * Set interface to work correctly. However, we have no straightforward
		 * way of consistently assigning one object as greater than the other if
		 * they are not equal (as per equals) but their hashCodes are equal, so
		 * we just treat them as equal in rank if their hashCodes are equal.
		 * This means that the Set interface may not work correctly here.
		 */
		@Override
		public int compareTo(GCConcurrentHashMap<K, V>.TimeKey o) {
			if (this.time == o.time)
				if (this.key.hashCode() > o.hashCode())
					return 1;
				else if (this.key.hashCode() < o.hashCode())
					return -1;
				else if (this.key.equals(o.key))
					return 0;
				else
					// what to do here?
					return 0;

			return this.time > o.time ? 1 : -1;
		}

		public boolean equals(TimeKey tk) {
			return this.key.equals(tk) && this.time == tk.time;
		}

		public int hashCode() {
			return this.key.hashCode() + (int) this.time;
		}

		public String toString() {
			return this.key.toString() + ":" + this.time;
		}
	}

	/**
	 * @param callback
	 * @param gcTimeout
	 */
	public GCConcurrentHashMap(GCConcurrentHashMapCallback callback,
			long gcTimeout) {
		super();
		this.callback = callback;
		this.gcTimeout = gcTimeout;
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public V put(K key, V value) {
		this.putGC(key);
		return super.put(key, value);
	}

	public V putIfAbsent(K key, V value) {
		this.putGC(key);
		return super.putIfAbsent(key, value);
	}

	public void putAll(Map<? extends K, ? extends V> map) {
		for (K key : map.keySet())
			this.putGC(key);
		super.putAll(map);
	}

	private synchronized void putGC(K key) {
		this.putTimes.put(key, System.currentTimeMillis());
		TimeKey tk = new TimeKey(System.currentTimeMillis(), key);
		this.putKeys.remove(tk);
		this.putKeys.add(tk);
		if (this.putKeys.size() > GC_THRESHOLD_SIZE || Util.oneIn(100))
			GC();
	}

	private synchronized void GC() {
		for (Iterator<TimeKey> tkIter = this.putKeys.iterator(); tkIter
				.hasNext();) {
			TimeKey tk = tkIter.next();
			if (System.currentTimeMillis() - tk.time > gcTimeout) {
				V value = this.remove(tk.key);
				this.putTimes.remove(tk.key);
				this.putKeys.remove(tk);
				this.callback.callbackGC(tk.key, value);
			} else
				break;
		}
	}

	/**
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException {
		Util.assertAssertionsEnabled();
		GC_THRESHOLD_SIZE = 5;
		GCConcurrentHashMap<String, Integer> map1 = new GCConcurrentHashMap<String, Integer>(
				new GCConcurrentHashMapCallback() {

					@Override
					public void callbackGC(Object key, Object value) {
						System.out.println("GC: " + key + ", " + value);
					}

				}, 1000);
		int n = 10;
		String prefix = "random";
		for (int i = 0; i < n; i++) {
			map1.put(prefix + i, (int) (Math.random() * Integer.MAX_VALUE));
			assert (map1.containsKey(prefix + i));
		}
		Thread.sleep(1500);
		map1.put(prefix + (n + 1), (int) (Math.random() * Integer.MAX_VALUE));
		for (int i = 0; i < n; i++)
			assert (!map1.containsKey(prefix + i)) : prefix + i;
	}
}
