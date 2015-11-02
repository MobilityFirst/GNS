package edu.umass.cs.utils;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * @author arun
 * 
 * @param <K>
 * @param <V>
 *
 *            DiskMap implements the ConcurrentMap<K,V> interface and allows
 *            applications to maintain very large maps limited only by the
 *            available disk space by automatically stowing away infrequently
 *            used map entries to disk. DiskMap is not a database and does not
 *            try to guarantee durability under crashes. However, if the map is
 *            closed gracefully, it will ensure durability by committing entries
 *            to disk before closing.
 * 
 *            DiskMap itself does not implement any disk I/O but relies on the
 *            application that must implement {@link #commit(Map)} and
 *            {@link #restore(Object)} methods that respectively write to and
 *            read from the disk.
 * 
 *            DiskMap is nearly indistinguishable in performance from
 *            ConcurrentHashMap when all map entries fit in memory. If not, its
 *            performance depends on how frequently entries are reused. If
 *            entries are rarely reused, e.g., a sweep over a very large number
 *            of map entries, then every access will on average force a disk
 *            read (to unpause the entry being accessed) and a write (to pause
 *            long idle entries). Currently, only entries that have been idle
 *            for at least {@code idleThreshold} can be paused to disk, so the
 *            caller must use {@link #setIdleThreshold(long)} so as to ensure
 *            that either no more than {@code sizeThreshold} entries are
 *            accessed in that interval or there is sufficient memory to hold
 *            all entries accessed in the last {@code idleThreshold} time.
 */
public abstract class DiskMap<K, V> implements ConcurrentMap<K, V>,
		Diskable<K, V> {

	/**
	 * 
	 */
	public static final int DEFAULT_CAPACITY = 1024 * 64;

	private final ConcurrentMap<K, V> map;

	private final ConcurrentMap<K, V> pauseQ = new ConcurrentHashMap<K, V>();

	/**
	 * This map could either be a MultiArrayMap or LinkedHashMap depending on
	 * whether {@link #enablePriorityQueue()} is enabled.
	 */
	private Map<Object, LastActive> stats;

	private final ScheduledExecutorService GC = Executors
			.newScheduledThreadPool(1, new ThreadFactory() {
				@Override
				public Thread newThread(Runnable r) {
					Thread thread = Executors.defaultThreadFactory().newThread(
							r);
					thread.setName(DiskMap.class.getSimpleName() + "-GC");
					return thread;
				}
			});

	private final int capacityEstimate;
	private long idleThreshold = 30000;
	private long pauseThreadPeriod = 30000;
	private long lastGCAttempt = 0;
	private boolean ongoingGC = false;

	/**
	 * Minimum idle time in order to be pausable.
	 * 
	 * @param idleTime
	 */
	public void setIdleThreshold(long idleTime) {
		this.idleThreshold = idleTime;
	}

	/**
	 * 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public synchronized void enablePriorityQueue() {
		if (this.stats instanceof LinkedHashMap)
			return;
		LinkedHashMap<Object, LastActive> tmpStats = new LinkedHashMap<Object, LastActive>();
		if (this.stats != null)
			for (Iterator<LastActive> iterLA = ((MultiArrayMap) this.stats)
					.concurrentIterator(); iterLA.hasNext();) {
				LastActive la = iterLA.next();
				tmpStats.put(la.key, la);
			}
		this.stats = tmpStats;
	}

	/**
	 * Period after which a pausing attempt is made by the GC thread.
	 * 
	 * @param period
	 */
	public void setPauseThreadPeriod(long period) {
		this.pauseThreadPeriod = period;
	}

	class LastActive implements Keyable<Object> {

		final Object key;
		long lastActive = System.currentTimeMillis();

		LastActive(Object key) {
			this.key = key;
		}

		@Override
		public Object getKey() {
			return this.key;
		}

		LastActive justActive() {
			this.lastActive = System.currentTimeMillis();
			return this;
		}
	}

	private void initPeriodicGC() {
		this.GC.scheduleWithFixedDelay(new TimerTask() {
			@Override
			public void run() {
				DiskMap.this.GC(false);
			}
		}, 0, pauseThreadPeriod, TimeUnit.MILLISECONDS);
	}

	private void initOnetimeGC() {
		DiskMap.this.GC(true);
	}

	/**
	 * @param capacityEstimate
	 *            Capacity estimate for in-memory map.
	 */
	public DiskMap(int capacityEstimate) {
		this.map = new ConcurrentHashMap<K, V>();
		this.capacityEstimate = capacityEstimate;
		// initStats();
		this.initPeriodicGC();
	}

	/**
	 * The supplied {@code map} will be used as the underlying in-memory map.
	 * From this point onwards, making direct modifications to this underlying
	 * map is unsafe.
	 * 
	 * @param map
	 */
	public DiskMap(ConcurrentMap<K, V> map) {
		this.map = map;
		this.capacityEstimate = map instanceof MultiArrayMap ? ((MultiArrayMap<?, ?>) map)
				.capacity() : DEFAULT_CAPACITY;
		// initStats();
		this.initPeriodicGC();
	}

	private void initStats() {
		if (this.map instanceof MultiArrayMap)
			this.stats = new MultiArrayMap<Object, LastActive>(
					map instanceof MultiArrayMap ? ((MultiArrayMap<?, ?>) map)
							.capacity() : DEFAULT_CAPACITY);
		else
			this.stats = new MultiArrayMap<Object, LastActive>(capacityEstimate);
	}

	/**
	 * This method should return only after successfully persisting the
	 * key,value pair, otherwise it should throw an exception.
	 * 
	 * @throws IOException
	 */
	abstract public Set<K> commit(Map<K, V> toCommit) throws IOException;

	/**
	 * @param key
	 * @return Value for key restored from persistent store.
	 * @throws IOException
	 */
	abstract public V restore(K key) throws IOException;

	@Override
	public int size() {
		return this.map.size();
	}

	@Override
	public boolean isEmpty() {
		return this.map.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		// need to check disk here
		return this.map.containsKey(key) || this.get(key) != null;
	}

	@Override
	public boolean containsValue(Object value) {
		if (!(value instanceof Keyable<?>))
			throw new RuntimeException(
					"containsValue(value) can only be invoked on a value of type Keyable<?>");
		return (this.map.containsValue(value) || this.get(
				((Keyable<?>) value).getKey()).equals(value));
	}

	@Override
	public V get(Object key) {
		V value = this.map.get(key);
		if (value == null)
			value = this.getOrRestore(key);
		if (value != null)
			this.markActive(key);
		if(this.shouldGC(true))
			this.initOnetimeGC();
		return value;
	}

	private void markActive(Object key) {
		if (this.stats == null)
			this.initStats();
		LastActive la = this.stats.get(key);
		if (la == null) {
			la = new LastActive(key);
		} else {
			la.justActive();
		}
		this.stats.put(key, la);
	}

	@Override
	public V put(K key, V value) {
		if (!(value instanceof Pausable))
			this.markActive(key);
		if (this.shouldGC(true))
			this.initOnetimeGC();
		assert(value!=null) : key;
		return this.map.put(key, value);
	}

	@Override
	public synchronized V remove(Object key) {
		// will try to get from disk if needed
		if (!this.containsKey(key))
			return null;

		if (this.stats != null)
			this.stats.remove(key);
		Map<K, V> entryAsMap = this.removeEntry(key);
		V value = null;
		if (entryAsMap != null && !entryAsMap.isEmpty()) {
			Map.Entry<K, V> entry = entryAsMap.entrySet().iterator().next();
			value = entry.getValue();
			entryAsMap.put(entry.getKey(), null);
			try {
				this.commit(entryAsMap);
			} catch (IOException e) {
				// any better option here?
				throw new RuntimeException(e.getMessage());
			}
		}
		return value;
	}

	/*
	 * There is seemingly no easy way to get the key of type K from the supplied
	 * key of type Object. So this method has to iterate over all entries to get
	 * the key in the map. If it is a MultiArrayMap, we can just get the key
	 * from the value.
	 */
	@SuppressWarnings("unchecked")
	private Map<K, V> removeEntry(Object key) {
		if (this.map.containsKey(key)) {
			HashMap<K, V> entryAsMap = new HashMap<K, V>();
			if (this.map instanceof MultiArrayMap) {
				V value = this.map.remove(key);
				if (value != null)
					entryAsMap.put(((Keyable<K>) value).getKey(), value);
				return entryAsMap;
			} else {
				K removedKey = null;
				V value = null;
				try {
					removedKey = (K) key;
					value = this.map.remove(key);
				} catch (ClassCastException cce) {
					for (Iterator<Map.Entry<K, V>> entryIter = this.map
							.entrySet().iterator(); entryIter.hasNext();) {
						Map.Entry<K, V> entry = entryIter.next();
						if (entry.getKey().equals(key)) {
							removedKey = entry.getKey();
							value = entry.getValue();
							entryIter.remove();
							break;
						}
					}
				}
				if (removedKey != null)
					entryAsMap.put(removedKey, value);
			}
		}
		return null;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		for (K key : map.keySet())
			this.markActive(key);
		this.map.putAll(m);
	}

	@Override
	public void clear() {
		if (this.stats != null)
			this.stats.clear();
		this.map.clear();
	}

	/**
	 * This method only returns the in-memory key set. There is currently no
	 * method to get the on-disk keys.
	 */
	@Override
	public Set<K> keySet() {
		return this.map.keySet();
	}

	/**
	 * This method only returns the in-memory value set. There is currently no
	 * method to get the on-disk values.
	 */
	@Override
	public Collection<V> values() {
		return this.map.values();
	}

	/**
	 * This method only returns the in-memory entry set. There is currently no
	 * method to get the on-disk entries.
	 */
	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return this.map.entrySet();
	}

	protected synchronized void hintRestore(K key, V value) {
		if (!this.map.containsKey(key) && !this.pauseQ.containsKey(key))
			this.map.put(key, value);
	}

	// get from map or restore from pauseQ or disk
	@SuppressWarnings("unchecked")
	private V getOrRestore(Object key) {
		V value = null;
		if ((value = this.map.get(key)) != null)
			return value;
		else if ((value = this.pauseQ.get(key)) != null) {
			// try restore from pauseQ
			try {
				synchronized (this) {
					this.map.put((K) key, value);
					this.pauseQ.remove(key);
				}
			} catch (ClassCastException e) {
				// do nothing
			}
		} else {
			// try restore from disk
			try {
				synchronized (this) {
					if ((value = this.restore((K) key)) != null)
						this.map.put((K) key, value);
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassCastException e) {
				// do nothing
			}
		}
		return value;
	}

	private boolean longIdle(LastActive la) {
		return System.currentTimeMillis() - la.lastActive > idleThreshold;
	}

	private static final int FORCE_PAUSE_FACTOR = 20;

	private boolean enqueuePause(K key) {
		V value = this.map.get(key);
		LastActive la = null;
		if (((value instanceof Pausable) && ((Pausable) value).isPausable())
		// if value is not Pausable, stats must be non-null
				|| (la = this.stats.get(key)) != null && longIdle(la)
				// pause at least one entry
				|| this.pauseQ.size() < this.capacityEstimate / FORCE_PAUSE_FACTOR) {
			synchronized (this) {
				value = this.map.remove(key);
				this.pauseQ.put(key, value);
				return true;
			}
		}
		return false;
	}
	private boolean shouldGC(boolean strict) {
		synchronized (this) {
			if (strict)
				return this.map.size() >= this.capacityEstimate;
			if (this.map.size() > this.capacityEstimate
					&& System.currentTimeMillis() - lastGCAttempt < this.pauseThreadPeriod)
				return true;
			return false;
		}
	}

	// pause long idle entries
	@SuppressWarnings("unchecked")
	private void GC(boolean strict) {
		synchronized(this) {
			if(!shouldGC(strict)) return;
			// else
			while (this.ongoingGC)
				try {
					wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			this.lastGCAttempt = System.currentTimeMillis();
			this.ongoingGC = true;
		}
		long t = System.currentTimeMillis();
		if (this.stats instanceof LinkedHashMap) {
			for (Iterator<LastActive> iterLA = this.stats.values().iterator(); iterLA
					.hasNext();) {
				if (!this.enqueuePause((K) (iterLA.next().key)))
					break;
			}
		}

		else {
			if (this.map instanceof MultiArrayMap) {
				for (Iterator<Keyable<K>> iterV = (Iterator<Keyable<K>>) (((MultiArrayMap<?, ?>) this.map)
						.concurrentIterator()); iterV.hasNext();) {
					this.enqueuePause(iterV.next().getKey());
				}
			} else {
				for (Iterator<K> iterK = this.map.keySet().iterator(); iterK
						.hasNext();) {
					this.enqueuePause(iterK.next());
				}
			}
		}

		if (!this.pauseQ.isEmpty()) {
			Set<K> committed = null;
			try {
				committed = this.commit(this.pauseQ);
			} catch (IOException e) {
				e.printStackTrace();
			}

			// remove paused key,value pairs from the map
			if (committed != null)
				for (K key : committed) {
					synchronized (this) {
						this.map.remove(key, this.pauseQ.remove(key));
						this.stats.remove(key);
					}
				}
			
			DelayProfiler.updateDelay("GC", t);
		}
		synchronized (this) {
			this.ongoingGC = false;
		}
	}

	@Override
	public synchronized V putIfAbsent(K key, V value) {
		if (!this.containsKey(key))
			return this.put(key, value);
		return null;
	}

	@Override
	public boolean remove(Object key, Object value) {
		if (!this.get(key).equals(value))
			return false;
		this.map.remove(key, value);
		return true;
	}

	@Override
	public synchronized boolean replace(K key, V oldValue, V newValue) {
		if (!this.get(key).equals(oldValue))
			return false;
		this.put(key, newValue);
		return true;
	}

	@Override
	public synchronized V replace(K key, V value) {
		if (this.containsKey(key))
			return this.put(key, value);
		return null;
	}

	/*
	 * There is no way to ensure that the value pointed to in the map is not
	 * modified after it has been paused to disk. It does not help to verify
	 * that the value pointed to is the same as the serialized value paused to
	 * disk as the value pointed to in the map could be modified by a caller
	 * even after its serialized form has been paused to disk. As a result, we
	 * can not support the "expectation" that modifications to the value object
	 * by the caller will be reflected in the map.
	 * 
	 * The caller is forced to reckon with the possibility that any
	 * modifications to the value object may be lost unless the caller
	 * explicitly invokes a put subsequently.
	 */

	@SuppressWarnings("unchecked")
	private Map<K, V> extractIterableMap(ConcurrentMap<K, V> m, int size) {
		if (m instanceof MultiArrayMap) {
			ConcurrentHashMap<K, V> chmap = new ConcurrentHashMap<K, V>();
			MultiArrayMap<K, ?> mamap = (MultiArrayMap<K, ?>) m;
			int count = 0;
			for (Iterator<?> iter = mamap.concurrentIterator(); iter.hasNext(); count++) {
				Keyable<K> value = (Keyable<K>) iter.next();
				chmap.put(value.getKey(), (V) value);
				if (count == size - 1)
					break;
			}
			return chmap;
		} else
			return m;
	}

	protected synchronized Set<K> commit(K key) throws IOException {
		Map<K, V> singleton = new ConcurrentHashMap<K, V>();
		if (this.map.containsKey(key))
			singleton.put(key, this.map.get(key));
		return this.commit(singleton);
	}

	/**
	 * Commits all in-memory entries in the map.
	 */
	public synchronized void commit() {
		this.commitAll(this.map, true);
	}
	
	public String toString() {
		return this.map.toString();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private synchronized void commitAll(Map<K, V> m, boolean clone) {
		ConcurrentMap<K, V> copy = new ConcurrentHashMap<K, V>();
		if (m instanceof MultiArrayMap) {
			for (Iterator<V> iter = (Iterator<V>) ((MultiArrayMap) m)
					.concurrentIterator(); iter.hasNext();) {
				V value = iter.next();
				if(value!=null)
					copy.put(((Keyable<K>) value).getKey(), value);
			}
		} else
			copy.putAll(m);

		while (!copy.isEmpty()) {
			Set<K> committed = null;
			try {
				committed = this.commit(this.extractIterableMap(copy,
						BATCH_SIZE));
			} catch (IOException e) {
				e.printStackTrace();
			}
			if (committed != null)
				for (K key : committed)
					copy.remove(key);
		}
	}

	private static final int BATCH_SIZE = 100;

	/**
	 * Will stop the GC thread but will have no other effect. The map can
	 * continue to be used with the same semantics.
	 */
	public void close() {
		this.GC.shutdown();
		this.commitAll(this.map, false);
	}
	
	private static class KeyableString implements Keyable<String> {
		final String key;
		final String value;
		KeyableString(String k, String v) {
			this.key = k;
			this.value = v;
		}
		@Override
		public String getKey() {
			return key;
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ConcurrentHashMap<String, String> db = new ConcurrentHashMap<String, String>();
		int dbSize = 4000*1000;
		MultiArrayMap<String, KeyableString> gt = new MultiArrayMap<String, KeyableString>(dbSize);
		boolean sleep = true;
		int capacity = 1000;
		DiskMap<String, String> dmap = new DiskMap<String, String>(capacity) {

			@Override
			public Set<String> commit(Map<String, String> toCommit)
					throws IOException {
				try {
					if (sleep)
						Thread.sleep(toCommit.size());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				db.putAll(toCommit);
				return new HashSet<String>(toCommit.keySet());
			}

			@Override
			public String restore(String key) throws IOException {
				try {
					if (sleep)
						Thread.sleep(1);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				return db.get(key);
			}
		};
		int n = dbSize;
		String s = "fbsldfsdnl";
		long t = System.currentTimeMillis();
		System.out.println("Starting test with n=" + n);
		int count=0;	
		while(true) {
			int i = (int)(Math.random()*(n));
			count++;
			if (count % 1000 == 0)
				System.out.println("put rate = "
						+ Util.df(count * 1000.0
								/ (System.currentTimeMillis() - t))
						+ DelayProfiler.getStats());
			
			if(Math.random() < 0.5 )  {
				String srand = s + Math.random()*Long.MAX_VALUE;
				dmap.put(i + "", srand);
				gt.put(i+"", new KeyableString(i+"", srand));
				//assert(dmap.get(i+"").equals(srand));
			}
			else {
				String retrieved = dmap.get(i+"");
				assert (retrieved == null && gt.get(i + "") == null || (retrieved != null && gt.get(i+"")!=null && retrieved
						.equals(gt.get(i + "").value))) : retrieved + " != " + gt.get(i+"") + " at count="+count;
			}
			assert(dmap.map.size() <= capacity) : dmap.map.size();
		}
		//dmap.close();
	}
}