package edu.umass.cs.utils;

import java.util.BitSet;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author V. Arun
 * @param <K>
 * @param <V>
 * 
 *            This class implements a cuckoo hashmap for storing objects
 *            implementing the Keyable<K> interface. It's main benefit is that
 *            it adds ~5 bytes of extra overhead per object (compared to
 *            {@code util.java.HashMap} or other that can use up to hundreds of
 *            bytes of additional overhead).
 *            <p>
 * 
 *            It supports two ways of iterating over the values in the map. The
 *            first is a more traditional iterator. The second is a
 *            non-fail-fast iterator that allows iteration as well as
 *            {@code remove} concurrently with other put/remove operations. The
 *            iteration may miss concurrently added elements (but will not
 *            return concurrently removed elements any time after they have been
 *            removed. This non-fail-fast iterator is useful for doing an
 *            "approximate" sweep over the entire map.
 */

public class MultiArrayMap<K, V extends Keyable<K>> implements Iterable<V>,
		ConcurrentMap<K, V> {
	private static final float SHRINKAGE = 0.75F;
	private static final int LEVELS = 6;
	private final int arraySize;
	// private final int levels;
	private Object[][] aMap;
	private BitSet[] bitsets;
	private HashMap<K, V> hMap;
	private int size = 0;
	private int modCount = 0;

	private static Logger log = Logger.getLogger(MultiArrayMap.class.getName());

	/**
	 * @param size
	 * @param levels
	 */
	public MultiArrayMap(int size, int levels) {
		this.arraySize = size;
		aMap = new Object[levels][];
		bitsets = new BitSet[levels];
		double sizeFactor = 1;
		for (int i = 0; i < levels; i++) {
			aMap[i] = new Object[(int) (sizeFactor * this.arraySize)];
			bitsets[i] = new BitSet((int) (sizeFactor * this.arraySize));
			sizeFactor *= SHRINKAGE;
		}
		// cop out map
		hMap = new HashMap<K, V>();

	}

	/**
	 * @param size
	 */
	public MultiArrayMap(int size) {
		this(size, LEVELS);
	}

	/**
	 * @return The size of the underlying array.
	 */
	public int capacity() {
		return this.arraySize;
	}

	/**
	 * @param key
	 * @param value
	 */
	@SuppressWarnings("unchecked")
	public synchronized V put(K key, V value) {
		assert (key.equals(value.getKey())) : key + " != " + value.getKey();
		boolean inserted = false;
		int level = 0;
		V prev = null;
		for (Object[] array : this.aMap) {
			int index = getHashIndex(key, array);
			if (array[index] == null
					|| (array[index] instanceof Keyable<?> && key
							.equals(((Keyable<?>) array[index]).getKey()))) {
				if (array[index] == null)
					size++;
				else
					log.log(Level.FINE, "{0} overwrote {1} in [{2},{3}]",
							new Object[] { this, value, level, index });
				prev = (V) (array[index]);
				array[index] = value;
				bitsets[level].set(index);
				inserted = true;
				modCount++;
				break;
			}
			level++;
		}
		if (!inserted) {
			prev = hMap.put(key, value);
			inserted = true;
		}
		return prev;
	}

	/**
	 * @param key
	 * @return The value to which the key maps.
	 */
	@SuppressWarnings("unchecked")
	public synchronized V get(Object key) {
		Object[] array = getArray(key);
		int index = getIndex(key, array);
		assert (index == -1 || array[index] != null);

		V foundValue = null;
		if (index >= 0) {
			foundValue = (V) array[index];
		} else {
			foundValue = this.hMap.get(key);
		}
		return foundValue;
	}

	/**
	 * @param key
	 * @return True if key is present.
	 */
	public synchronized boolean containsKey(Object key) {
		return (key != null && get(key) != null);
	}

	/**
	 * @param value
	 * @return True if value present.
	 */
	public synchronized boolean containsValue(Object value) {
		return (value != null && (value instanceof Keyable<?>) && get(
				((Keyable<?>) value).getKey()).equals(value));
	}

	/**
	 * @param key
	 * @return Previous value if any.
	 */
	public synchronized V remove(Object key) {

		V value = get(key);
		if (value == null)
			return null;

		int level = this.getLevel(key);
		int index = -1;
		Object[] array = null;
		if (level >= 0) {
			array = this.getArray(key);
			index = getIndex(key, array);
		}
		assert (index == -1 || array[index] != null);

		if (index >= 0) {
			array[index] = null;
			this.bitsets[level].clear(index);
		} else
			this.hMap.remove(key);

		modCount++;
		this.size--;
		return value;
	}

	/**
	 * @return The size.
	 */
	public synchronized int size() {
		return this.size + this.hMap.size();
	}

	/**
	 * @return The hashmap size.
	 */
	public synchronized int hashmapSize() {
		return this.hMap.size();
	}

	/**
	 * Resets the map.
	 */
	public synchronized void clear() {
		for (Object[] array : this.aMap)
			for (int i = 0; i < array.length; i++)
				array[i] = null;
		this.hMap.clear();
		for (BitSet bitset : this.bitsets)
			bitset.clear();
		this.size = 0;
	}

	@Override
	public boolean isEmpty() {
		return this.size() == 0;
	}

	@Override
	public synchronized void putAll(Map<? extends K, ? extends V> m) {
		for (K key : m.keySet())
			this.put(key, m.get(key));
	}

	@SuppressWarnings("unchecked")
	@Override
	public Set<K> keySet() {
		return new Set<K>() {

			@Override
			public int size() {
				return MultiArrayMap.this.size();
			}

			@Override
			public boolean isEmpty() {
				return MultiArrayMap.this.isEmpty();
			}

			@Override
			public boolean contains(Object o) {
				return MultiArrayMap.this.containsKey(o);
			}

			@Override
			public Iterator<K> iterator() {
				final Iterator<V> iterV = MultiArrayMap.this.iterator();
				return new Iterator<K>() {

					@Override
					public boolean hasNext() {
						return iterV.hasNext();
					}

					@Override
					public K next() {
						return iterV.next().getKey();
					}
				};
			}

			@Override
			public Object[] toArray() {
				Set<K> keys = new HashSet<K>();
				for (Iterator<V> iterV = MultiArrayMap.this
						.concurrentIterator(); iterV.hasNext();) {
					V value = iterV.next();
					if (value != null)
						keys.add(value.getKey());
				}
				return keys.toArray();
			}

			@Override
			public Object[] toArray(Object[] a) {
				Set<K> keys = new HashSet<K>();
				for (Iterator<V> iterV = MultiArrayMap.this
						.concurrentIterator(); iterV.hasNext();) {
					V value = iterV.next();
					if (value != null)
						keys.add(value.getKey());
				}
				return keys.toArray(a);
			}

			@Override
			public boolean add(Object e) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean remove(Object o) {
				return MultiArrayMap.this.remove(o) != null;
			}

			@Override
			public boolean containsAll(
					@SuppressWarnings("rawtypes") Collection c) {
				boolean contains = true;
				for (Object o : c)
					contains = contains && MultiArrayMap.this.containsKey(o);
				return contains;
			}

			@Override
			public boolean addAll(@SuppressWarnings("rawtypes") Collection c) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean retainAll(@SuppressWarnings("rawtypes") Collection c) {
				boolean removed = false;
				for (Iterator<V> iterV = MultiArrayMap.this
						.concurrentIterator(); iterV.hasNext();) {
					V value = iterV.next();
					if (value != null && !c.contains(value.getKey()))
						removed = removed
								|| (MultiArrayMap.this.remove(value.getKey()) != null);
				}
				return removed;
			}

			@Override
			public boolean removeAll(@SuppressWarnings("rawtypes") Collection c) {
				boolean removed = false;
				for (Object o : c)
					removed = removed || (MultiArrayMap.this.remove(o) != null);
				return removed;
			}

			@Override
			public void clear() {
				MultiArrayMap.this.clear();
			}
		};
	}

	@Override
	public Collection<V> values() {
		return new Set<V>() {

			@Override
			public int size() {
				return MultiArrayMap.this.size;
			}

			@Override
			public boolean isEmpty() {
				return MultiArrayMap.this.isEmpty();
			}

			@Override
			public boolean contains(Object o) {
				return MultiArrayMap.this.containsValue(o);
			}

			@Override
			public Iterator<V> iterator() {
				return MultiArrayMap.this.iterator();
			}

			@Override
			public Object[] toArray() {
				Set<V> values = new HashSet<V>();
				for (Iterator<V> iterV = MultiArrayMap.this
						.concurrentIterator(); iterV.hasNext();) {
					V value = iterV.next();
					if (value != null)
						values.add(value);
				}
				return values.toArray();
			}

			@Override
			public <T> T[] toArray(T[] a) {
				Set<V> values = new HashSet<V>();
				for (Iterator<V> iterV = MultiArrayMap.this
						.concurrentIterator(); iterV.hasNext();) {
					V value = iterV.next();
					if (value != null)
						values.add(value);
				}
				return values.toArray(a);
			}

			@Override
			public boolean add(V e) {
				throw new UnsupportedOperationException();
			}

			@SuppressWarnings("unchecked")
			@Override
			public boolean remove(Object o) {
				return MultiArrayMap.this.remove(((Keyable<K>) o).getKey()) != null;
			}

			@Override
			public boolean containsAll(Collection<?> c) {
				boolean contains = true;
				for (Object o : c)
					contains = contains && MultiArrayMap.this.containsValue(o);
				return contains;
			}

			@Override
			public boolean addAll(Collection<? extends V> c) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean retainAll(Collection<?> c) {
				boolean removed = false;
				for (Iterator<V> iterV = MultiArrayMap.this
						.concurrentIterator(); iterV.hasNext();) {
					V value = iterV.next();
					if (value != null && !c.contains(value))
						removed = removed
								|| (MultiArrayMap.this.remove(value.getKey()) != null);
				}
				return removed;
			}

			@Override
			public boolean removeAll(Collection<?> c) {
				boolean removed = false;
				for (Object o : c)
					removed = removed
							|| (o instanceof Keyable<?> && MultiArrayMap.this
									.remove(((Keyable<?>) o).getKey()) != null);
				return removed;
			}

			@Override
			public void clear() {
				MultiArrayMap.this.clear();
			}
		};
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return new Set<Map.Entry<K, V>>() {

			@Override
			public int size() {
				return MultiArrayMap.this.size();
			}

			@Override
			public boolean isEmpty() {
				return MultiArrayMap.this.isEmpty();
			}

			@SuppressWarnings("rawtypes")
			@Override
			public boolean contains(Object o) {
				return (o instanceof Map.Entry)
						&& MultiArrayMap.this.containsKey(((Map.Entry) o)
								.getKey());
			}

			@Override
			public Iterator<java.util.Map.Entry<K, V>> iterator() {
				final Iterator<V> iterV = MultiArrayMap.this.iterator();
				return new Iterator<Map.Entry<K, V>>() {

					@Override
					public boolean hasNext() {
						return iterV.hasNext();
					}

					@Override
					public java.util.Map.Entry<K, V> next() {
						V value = iterV.next();
						return new Map.Entry<K, V>() {

							@Override
							public K getKey() {
								return value != null ? value.getKey() : null;
							}

							@Override
							public V getValue() {
								return value;
							}

							@Override
							public V setValue(V newValue) {
								return value != null ? MultiArrayMap.this.put(
										value.getKey(), newValue) : null;
							}
						};
					}

				};
			}

			@Override
			public Object[] toArray() {
				return this.getEntrySet().toArray();
			}

			private Set<Map.Entry<K, V>> getEntrySet() {
				Set<Map.Entry<K, V>> entries = new HashSet<Map.Entry<K, V>>();
				for (Iterator<V> iterV = MultiArrayMap.this
						.concurrentIterator(); iterV.hasNext();) {
					V value = iterV.next();
					if (value != null)
						entries.add(new Map.Entry<K, V>() {

							@Override
							public K getKey() {
								return value.getKey();
							}

							@Override
							public V getValue() {
								return value;
							}

							@Override
							public V setValue(V newValue) {
								return MultiArrayMap.this.put(value.getKey(),
										newValue);
							}
						});
				}
				return entries;
			}

			@Override
			public <T> T[] toArray(T[] a) {
				return this.getEntrySet().toArray(a);
			}

			@Override
			public boolean add(java.util.Map.Entry<K, V> e) {
				throw new UnsupportedOperationException();
			}

			@SuppressWarnings("rawtypes")
			@Override
			public boolean remove(Object o) {
				return (o instanceof Map.Entry)
						&& MultiArrayMap.this.remove(((Map.Entry) o).getKey()) != null;
			}

			@SuppressWarnings("rawtypes")
			@Override
			public boolean containsAll(Collection<?> c) {
				boolean contains = true;
				for (Object o : c)
					contains = contains
							&& (o instanceof Map.Entry)
							&& (MultiArrayMap.this.containsKey(((Map.Entry) o)
									.getKey()));
				return contains;
			}

			@Override
			public boolean addAll(
					Collection<? extends java.util.Map.Entry<K, V>> c) {
				throw new UnsupportedOperationException();
			}

			@SuppressWarnings("rawtypes")
			@Override
			public boolean retainAll(Collection<?> c) {
				boolean removed = false;
				for (Iterator<V> iterV = MultiArrayMap.this
						.concurrentIterator(); iterV.hasNext();) {
					V value = iterV.next();
					K key = value != null ? value.getKey() : null;
					for (Object o : c)
						if (key != null && value != null
								&& (o instanceof Map.Entry)
								&& (((Map.Entry) o).getKey().equals(key))
								&& (((Map.Entry) o).getValue().equals(value)))
							removed = removed
									|| MultiArrayMap.this.remove(key) != null;
				}
				return removed;
			}

			@SuppressWarnings("rawtypes")
			@Override
			public boolean removeAll(Collection<?> c) {
				boolean removed = false;
				for (Object o : c)
					removed = removed
							|| ((c instanceof Map.Entry) && MultiArrayMap.this
									.remove(((Map.Entry) o).getKey()) != null);
				return removed;
			}

			@Override
			public void clear() {
				MultiArrayMap.this.clear();
			}
		};
	}

	@Override
	public synchronized boolean remove(Object key, Object value) {
		if (this.get(key).equals(value))
			return this.remove(key) != null;
		return false;
	}

	@Override
	public boolean replace(K key, V oldValue, V newValue) {
		if (this.get(key).equals(oldValue))
			return this.put(key, newValue) != null;
		return false;
	}

	@Override
	public synchronized V replace(K key, V value) {
		if (this.containsKey(key))
			return this.put(key, value);
		return null;
	}

	/**
	 * @param key
	 * @param value
	 * @return Previous value mapped to {@code key} if any.
	 */
	@Override
	public synchronized V putIfAbsent(K key, V value) {
		if (this.containsKey(key))
			return this.get(key);
		this.put(key, value);
		return null;
	}

	private synchronized Object[] getArray(Object key) {
		Object[] foundArray = null;
		for (Object[] array : this.aMap) {
			int index = getIndex(key, array);
			if (index >= 0) {
				foundArray = array;
				break;
			}
		}
		return foundArray;
	}

	private synchronized int getLevel(Object key) {
		for (int i = 0; i < aMap.length; i++) {
			int index = getIndex(key, aMap[i]);
			if (index >= 0) {
				return i;
			}
		}
		return -1;
	}

	private synchronized int getIndex(Object key, Object[] array) {
		int foundIndex = -1;
		if (array != null) {
			int index = getHashIndex(key, array);
			if (array[index] != null && array[index] instanceof Keyable<?>
					&& key.equals(((Keyable<?>) array[index]).getKey())) {
				foundIndex = index;
			}
		}
		return foundIndex;
	}

	private synchronized int getHashIndex(Object key, Object[] array) {
		int hash = key.hashCode();
		int index = hash % array.length;
		if (index < 0)
			index += array.length;
		return index;
	}

	// read-only iterator allows concurrency
	class ConcurrentMAMIterator implements Iterator<V> {
		private int level = 0;
		private int index = -1;
		private V last = null;

		private Object[] hMapValues;
		private int hMapIndex = -1;

		/**
		 * Note: next() may return null immediately after hasNext() returns
		 * true. This behavior is unlike a traditional iterator wherein, if
		 * hasNext() returns true, next() necessarily returns a non-null value
		 * or throws a {@code ConcurrentModificationException}
		 */
		@Override
		public boolean hasNext() {
			if (arrayHasNext())
				return true;
			return hMapIndex == -1 ? !hMap.isEmpty()
					: hMapIndex < hMapValues.length;
		}

		private boolean arrayHasNext() {
			for (int i = level; i < MultiArrayMap.this.aMap.length; i++) {
				if (bitsets[i].nextSetBit(i == level ? index + 1 : 0) >= 0)
					return true;
			}
			return false;
		}

		/**
		 * Note: next() may return null immediately after hasNext() returns
		 * true. This behavior is unlike a traditional iterator wherein, if
		 * hasNext() returns true, next() necessarily returns a non-null value
		 * or throws a {@code ConcurrentModificationException}
		 */
		@SuppressWarnings("unchecked")
		@Override
		public V next() {
			V value = null;
			if ((value = arrayNext()) != null)
				return value;
			if (hMapIndex == -1
					&& (hMapValues = hMap.values().toArray()).length == 0)
				return null;
			return ++hMapIndex < hMapValues.length ? last = (V) hMapValues[hMapIndex]
					: null;
		}

		@SuppressWarnings("unchecked")
		private V arrayNext() {
			for (; level < MultiArrayMap.this.aMap.length; level++) {
				index = bitsets[level].nextSetBit(index + 1);
				if (index >= 0)
					return last = (V) aMap[level][index];
				else
					assert (index == -1);
			}
			return null;
		}

		public void remove() {
			removed();
		}

		private boolean removed() {
			if (last == null)
				return false;
			MultiArrayMap.this.remove(last.getKey());
			modCount++;
			return true;
		}
	}

	// traditional fail-fast under concurrency semantics, no cloning
	class MAMIterator extends ConcurrentMAMIterator implements Iterator<V> {
		Iterator<V> hMapIter = null;
		int expectedModCount = modCount;

		@Override
		public boolean hasNext() {
			if (modCount != expectedModCount)
				throw new ConcurrentModificationException();
			if (super.arrayHasNext())
				return true;
			return hMapIter == null ? !hMap.isEmpty() : hMapIter.hasNext();
		}

		@Override
		public V next() {
			if (modCount != expectedModCount)
				throw new ConcurrentModificationException();
			V value = null;
			if ((value = super.arrayNext()) != null)
				return value;
			if (hMapIter == null)
				hMapIter = hMap.values().iterator();
			return hMapIter.next();
		}

		public void remove() {
			if (modCount != expectedModCount)
				throw new ConcurrentModificationException();
			if (!super.removed())
				return;
			else
				expectedModCount++;
		}
	}

	@Override
	public Iterator<V> iterator() {
		return new MAMIterator();
	}

	/**
	 * @return Non-fail-fast iterator that allows concurrent modification but
	 *         with the caveat that next() may return null even though hasNext()
	 *         returns true. Concurrent add/remove operations may be overlooked.
	 */
	public Iterator<V> concurrentIterator() {
		return new ConcurrentMAMIterator();
	}

	static class StringValue<ValueType> implements Keyable<String> {
		final String key;
		final ValueType value;

		StringValue(String s, ValueType v) {
			key = s;
			value = v;
		}

		public String getKey() {
			return key;
		}

		public String toString() {
			return value.toString();
		}
	}

	@SuppressWarnings("unchecked")
	private static void createSimpleArray(int size) {
		StringValue<Integer>[] svarray = null;
		svarray = new StringValue[size]; // SuppressWarnings
		for (int i = 0; i < size; i++) {
			svarray[i] = new StringValue<Integer>("someRandomString" + i,
					i + 23);
		}
	}

	private static int testSum = 0;

	private static MultiArrayMap<String, StringValue<Integer>> createRandomMAM(
			int size) {
		System.out.print("Inserting " + size / (1000 * 1000)
				+ " million values");
		MultiArrayMap<String, StringValue<Integer>> map = new MultiArrayMap<String, StringValue<Integer>>(
				(int) (size), 6);
		assert (map.get("hello") == null);
		map.put("someRandomString0", new StringValue<Integer>(
				"someRandomString0", Integer.MAX_VALUE));
		assert (((StringValue<Integer>) map.get("someRandomString0")).value == Integer.MAX_VALUE);
		for (int i = 0; i < size; i++) {
			String key = "someRandomString" + i;
			int intValue = ((int) (Math.random() * Integer.MAX_VALUE));
			map.put(key, new StringValue<Integer>(key, intValue));
			testSum += intValue;
			assert (((StringValue<Integer>) map.get(key)).value == intValue);
			printProgressBar(i);
		}

		return map;
	}

	private static void printProgressBar(int i) {
		if (i % 200000 == 0)
			System.out.print(".");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Util.assertAssertionsEnabled();
		int million = 1000000;
		int size = (int) (10 * million);

		boolean simpleArray = false;
		MultiArrayMap<String, StringValue<Integer>> map = null;

		long t1 = System.currentTimeMillis();
		System.out.println("Initiating test...");
		if (simpleArray)
			createSimpleArray(size);
		map = createRandomMAM(size);
		System.out.println("succeeded (cop-out hashmap size = "
				+ map.hashmapSize() + "); time = "
				+ (System.currentTimeMillis() - t1) + "ms");

		System.out.print("Iterating(1)");
		int count = 0;
		int sum = 0;
		t1 = System.currentTimeMillis();
		count = sum = 0;
		for (Iterator<StringValue<Integer>> iter = map.concurrentIterator(); iter
				.hasNext();) {
			printProgressBar(count);
			sum += iter.next().value;
			count++;
		}
		assert (count == map.size() && sum == testSum) : count + " != "
				+ map.size();
		System.out.println("succeeded; time = "
				+ (System.currentTimeMillis() - t1) + "ms");

		System.out.print("Iterating(2)");
		t1 = System.currentTimeMillis();
		count = sum = 0;
		for (Iterator<StringValue<Integer>> iter = map.iterator(); iter
				.hasNext();) {
			printProgressBar(count);
			sum += iter.next().value;
			count++;
		}
		assert (count == map.size() && sum == testSum) : count + " != "
				+ map.size();
		System.out.println("succeeded; time = "
				+ (System.currentTimeMillis() - t1) + "ms");

	}
}
