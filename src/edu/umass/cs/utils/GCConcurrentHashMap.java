/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */

package edu.umass.cs.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author arun
 * @param <K>
 * @param <V>
 *
 */
public class GCConcurrentHashMap<K, V> extends ConcurrentHashMap<K, V> {

	private static final int DEFAULT_GC_TIMEOUT = 10000;
	private static final int DEFAULT_GC_THRESHOLD_SIZE = 1024 * 128;
	private int gcThresholdSize = DEFAULT_GC_THRESHOLD_SIZE;

	private final LinkedHashMap<K, Long> putTimes = new LinkedHashMap<K, Long>();
	private final GCConcurrentHashMapCallback callback;
	private final long gcTimeout;

	/**
	 * @param callback
	 * @param gcTimeout
	 */
	public GCConcurrentHashMap(GCConcurrentHashMapCallback callback,
			long gcTimeout) {
		super();
		this.callback = callback;
		this.gcTimeout = gcTimeout;
		this.minGCInterval = this.gcTimeout;
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 183021919212L;

	public synchronized V put(K key, V value) {
		this.putGC(key);
		V old = super.put(key, value);
		return old;
	}

	public synchronized V putIfAbsent(K key, V value) {
		this.putGC(key);
		return super.putIfAbsent(key, value);
	}

	public synchronized void putAll(Map<? extends K, ? extends V> map) {
		for (K key : map.keySet())
			this.putGC(key);
		super.putAll(map);
	}

	public synchronized V remove(Object key) {
		V value = super.remove(key);
		this.putTimes.remove(key);
		return value;
	}

	/**
	 * @param size
	 */
	public void setGCThresholdSize(int size) {
		this.gcThresholdSize = size;
	}

	public synchronized boolean remove(Object key, Object value) {
		if (super.remove(key, value)) {
			this.putTimes.remove(key);
			return true;
		}
		return false;
	}

	private synchronized void putGC(K key) {
		this.putTimes.put(key, System.currentTimeMillis());
		if (this.size() > gcThresholdSize || Util.oneIn(1000))
			GC();
	}

	private int numGC = 0;
	private int numGCAttempts = 0;
	private long lastGCTime = 0;
	private long minGCInterval = DEFAULT_GC_TIMEOUT;

	/**
	 * @param timeout 
	 */
	public synchronized void tryGC(long timeout) {
		this.GC(timeout);
	}
	private synchronized void GC() {
		this.GC(this.gcTimeout);
	}
	private synchronized void GC(long timeout) {
		if (System.currentTimeMillis() - this.lastGCTime < this.minGCInterval)
			return;
		else
			this.lastGCTime = System.currentTimeMillis();
		boolean removed = false;
		numGCAttempts++;
		for (Iterator<K> iterK = this.putTimes.keySet().iterator(); iterK
				.hasNext();) {
			K key = iterK.next();
			Long time = this.putTimes.get(key);
			if (time != null
					&& (System.currentTimeMillis() - time > timeout)) {
				iterK.remove();
				V value = this.remove(key);
				if (value != null)
					this.callback.callbackGC(key, value);
				removed = true;
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
		GCConcurrentHashMap<String, Integer> map1 = new GCConcurrentHashMap<String, Integer>(
				new GCConcurrentHashMapCallback() {

					@Override
					public void callbackGC(Object key, Object value) {
						// System.out.println("GC: " + key + ", " + value);
					}

				}, 100);
		map1.setGCThresholdSize(1000);
		ConcurrentHashMap<String, Integer> map2 = new ConcurrentHashMap<String, Integer>();
		Map<String, Integer> map = map1;
		HashMap<String, Integer> hmap = new HashMap<String, Integer>();
		int n = 1000 * 1000;
		String prefix = "random";
		long t = System.currentTimeMillis();
		for (int i = 0; i < n; i++) {
			(map != null ? map : hmap).put(prefix + i, i);
			assert ((map != null ? map : hmap).containsKey(prefix + i));
			int sizeThreshold = 8000;
			if (i >= sizeThreshold)
				map.remove(prefix + (i - sizeThreshold));
		}
		long t2 = System.currentTimeMillis();
		System.out.println("delay = " + (t2 - t) + "; rate = "
				+ (n / (t2 - t) + "K/s") + "; numGC = " + map1.numGC
				+ "; numGCAttempts = " + map1.numGCAttempts);
		System.out.println("size = " + map.size());
		Thread.sleep(1500);
		(map != null ? map : hmap).put(prefix + (n + 1),
				(int) (Math.random() * Integer.MAX_VALUE));
		for (int i = 0; i < n; i++)
			assert (!map1.containsKey(prefix + i)
					|| !map1.putTimes.containsKey(prefix + i) || (t2
					- map1.putTimes.get(prefix + i) < map1.gcTimeout)) : prefix
					+ i;
		assert (map1 != null && map2 != null);
	}
}
