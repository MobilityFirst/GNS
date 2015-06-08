package edu.umass.cs.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Logger;

/**
 * @author V. Arun
 * @param <KeyType> 
 * @param <ValueType> 
 */

public class MultiArrayMap<KeyType, ValueType extends Keyable<KeyType>> {
	private static final float SHRINKAGE = 0.75F;
	private static final int LEVELS = 6;
	private final int arraySize;
	private final int levels;
	private ArrayList<Object[]> aMap;
	private HashMap<KeyType, ValueType> hMap;
	private int count = 0;

	private static Logger log = Logger.getLogger(MultiArrayMap.class.getName()); 

	public MultiArrayMap(int size, int levels) {
		this.arraySize = size;
		this.levels = levels;
		initialize();
	}

	public MultiArrayMap(int size) {
		this.arraySize = size;
		this.levels = LEVELS;
		initialize();
	}

	private void initialize() {
		aMap = new ArrayList<Object[]>();
		double sizeFactor = 1;
		for (int i = 0; i < levels; i++) {
			Object[] array = new Object[(int) (sizeFactor * this.arraySize)];
			sizeFactor *= SHRINKAGE;
			aMap.add(array);
		}
		hMap = new HashMap<KeyType, ValueType>();

	}

	public synchronized void put(KeyType key, ValueType value) {
		assert (key.equals(value.getKey())) : key + " != " + value.getKey();
		boolean inserted = false;
		int level = 0;
		for (Object[] array : this.aMap) {
			int index = getHashIndex(key, array);
			if (array[index] == null
					|| (array[index] instanceof Keyable<?> && key
							.equals(((Keyable<?>) array[index]).getKey()))) {
				if (array[index] == null)
					count++;
				else
					log.info("Overwrote " + value + " in level " + level
							+ " index " + index);
				array[index] = value;
				inserted = true;
				break;
			}
			level++;
		}
		if (!inserted) {
			hMap.put(key, value);
			inserted = true;
		}
	}

	@SuppressWarnings("unchecked")
	public synchronized ValueType get(KeyType key) {
		Object[] array = getArray(key);
		int index = getIndex(key, array);
		assert (index == -1 || array[index] != null);

		ValueType foundValue = null;
		if (index >= 0) {
			// Not sure if there is a way to avoid the warning here
			foundValue = (ValueType) array[index]; // SuppressWarnings
		} else {
			foundValue = this.hMap.get(key);
		}
		return foundValue;
	}

	public synchronized boolean containsKey(KeyType key) {
		return (key != null && get(key) != null);
	}

	public synchronized ValueType remove(KeyType key) {
		ValueType value = (ValueType) get(key);
		if (value == null)
			return null;

		Object[] array = getArray(key);
		int index = getIndex(key, array);
		assert (index == -1 || array[index] != null);

		if (index >= 0) {
			array[index] = null;
		} else
			this.hMap.remove(key);

		this.count--;
		return value;
	}

	public synchronized int size() {
		return this.count + this.hMap.size();
	}

	public synchronized int hashmapSize() {
		return this.hMap.size();
	}

	public synchronized void clear() {
		this.aMap = null;
		this.hMap = null;
		this.count = 0;
	}

	private synchronized Object[] getArray(KeyType key) {
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

	private synchronized int getIndex(KeyType key, Object[] array) {
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

	private synchronized int getHashIndex(KeyType key, Object[] array) {
		int hash = key.hashCode();
		int index = hash % array.length;
		if (index < 0)
			index += array.length;
		return index;
	}

	/**
	 * @param args
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		int million = 1000000;
		int size = (int) (8.2 * million);
		class StringValue<ValueType> implements Keyable<String> {
			final String key;
			final ValueType value;

			StringValue(String s, ValueType v) {
				key = s;
				value = v;
			}

			public String getKey() {
				return key;
			}
		}
		boolean simpleArray = false;
		StringValue<Integer>[] svarray = null;
		MultiArrayMap<String, StringValue<Integer>> map = null;
		// HashMap<String,StringValue<Integer>> map = null;
		long t1 = System.currentTimeMillis();

		if (simpleArray) {
			svarray = new StringValue[size]; // SuppressWarnings
			for (int i = 0; i < size; i++) {
				svarray[i] = new StringValue<Integer>("paxos" + i, i + 23);
			}
		} else {
			map = new MultiArrayMap<String, StringValue<Integer>>((int) (size),
					6);
			// map = new HashMap<String,StringValue<Integer>>(size, 6);
			assert (map.get("hello") == null);
			map.put("paxos0", new StringValue<Integer>("paxos0", 1));
			assert (((StringValue<Integer>) map.get("paxos0")).value == 1);
			for (int i = 0; i < size; i++) {
				String key = "paxos" + i;
				map.put(key, new StringValue<Integer>(key, i + 23));
				assert (((StringValue<Integer>) map.get(key)).value == i + 23);
				// System.out.println("Successfully inserted " + key);
			}
			System.out.println("hashmapCount = " + map.hashmapSize());
		}
		System.out.println("Time = " + (System.currentTimeMillis() - t1));
	}

}
