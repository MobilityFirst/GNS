package edu.umass.cs.utils;

import java.util.HashMap;
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

	private static int GC_THRESHOLD_SIZE = 1024 * 128;

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
		V old = super.put(key, value);
		return old;
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

	public V remove(Object key) {
		return this.remove(key, false);
	}

	@SuppressWarnings("unchecked")
	private V remove(Object key, boolean fromGC) {
		V value = super.remove(key);
		Long time = this.putTimes.remove(key);

		if (!fromGC)
			return value;

		try {
			if (time != null)
				this.putKeys.remove(new TimeKey(time, (K) key));
		} catch (Exception e) {
			// do nothing, likely a class cast exception
		}
		return value;
	}

	@SuppressWarnings("unchecked")
	public boolean remove(Object key, Object value) {
		boolean removed = super.remove(key, value);
		Long time = removed ? this.putTimes.remove(key) : null;
		try {
			if (time != null)
				this.putKeys.remove(new TimeKey(time, (K) key));
		} catch (Exception e) {
			// do nothing, likely a class cast exception
		}
		return removed;
	}

	private synchronized void putGC(K key) {
		this.putTimes.put(key, System.currentTimeMillis());
		TimeKey tk = new TimeKey(System.currentTimeMillis(), key);
		/* Remove first because add only adds if not present and
		 * we need the new timekey (with the new time) to get
		 * inserted, otherwise keys previously inserted and deleted
		 * and then reinserted may get garbage collected needlessly.
		 */
		this.putKeys.remove(tk);
		this.putKeys.add(tk);
		if (this.putKeys.size() > GC_THRESHOLD_SIZE || Util.oneIn(1000))
			GC();
	}

	private static int numGC = 0;

	private synchronized void GC() {
		boolean removed = false;
		for (Iterator<TimeKey> tkIter = this.putKeys.iterator(); tkIter
				.hasNext();) {
			TimeKey tk = tkIter.next();
			if (System.currentTimeMillis() - tk.time > gcTimeout) {
				V value = this.remove(tk.key, true);
				tkIter.remove();
				removed = true;
				this.callback.callbackGC(tk.key, value);
			} else
				break;
		}
		if (removed)
			numGC++;
	}

	/**
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException {
		Util.assertAssertionsEnabled();
		// GC_THRESHOLD_SIZE = 5;
		GCConcurrentHashMap<String, Integer> map1 = new GCConcurrentHashMap<String, Integer>(
				new GCConcurrentHashMapCallback() {

					@Override
					public void callbackGC(Object key, Object value) {
						// System.out.println("GC: " + key + ", " + value);
					}

				}, 1000);
		ConcurrentHashMap<String, Integer> map2 = new ConcurrentHashMap<String, Integer>();
		ConcurrentHashMap<String, Integer> map = map2;
		HashMap<String, Integer> hmap = new HashMap<String, Integer>();
		int n = 1000 * 1000;
		String prefix = "random";
		long t = System.currentTimeMillis();
		for (int i = 0; i < n; i++) {
			(map != null ? map : hmap).put(prefix + i, i);
			assert ((map != null ? map : hmap).containsKey(prefix + i));
			if (i >= 5000)
				map.remove(prefix + (i - 5000));
		}
		System.out.println("delay = " + (System.currentTimeMillis() - t)
				+ "; rate = " + (n / (System.currentTimeMillis() - t) + "K/s")
				+ "; numGC = " + numGC);
		System.out.println("size = " + map.size());
		Thread.sleep(1500);
		(map != null ? map : hmap).put(prefix + (n + 1),
				(int) (Math.random() * Integer.MAX_VALUE));
		for (int i = 0; i < n; i++)
			assert (GC_THRESHOLD_SIZE > n / 100 || !(map != null ? map : hmap)
					.containsKey(prefix + i)) : prefix + i;

		assert (map1 != null && map2 != null);
	}
}
