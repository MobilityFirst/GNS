package edu.umass.cs.utils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
@author V. Arun
 * @param <KeyType> 
 * @param <ValueType> 
 */

/* This class is an optimized hashmap that maintains an underlying 
 * hashmap that is null if empty. An empty hashmap can take about 
 * 150 bytes, while an empty NullIfEmptyMap takes only a few bytes
 * for a couple pointers. This can make a big difference while 
 * maintaining say millions of hashmaps most of which are empty.
 * 
 *  By default, the hashmap uses an initial capacity of 2. This means 
 *  that there will be some performance overhead in expanding
 *  the map as needed upon a burst of requests. But this further helps
 *  shave off a 2x factor compared to a default hashmap capacity of 16.
 */
public class NullIfEmptyMap<KeyType,ValueType> {
	private static final int CAPACITY = 2;
	private HashMap<KeyType,ValueType> map = null;
	
	/**
	 * @param key
	 * @param value
	 */
	public synchronized void put(KeyType key, ValueType value){
		if(map==null) map = new HashMap<KeyType,ValueType>(NullIfEmptyMap.CAPACITY);
		map.put(key, value);
	}
	/**
	 * @param key
	 * @return The value for the key.
	 */
	public synchronized ValueType get(KeyType key) {
		if(map==null) return null;
		return map.get(key);
	}
	/**
	 * @param key
	 * @return The value removed.
	 */
	public synchronized ValueType remove(KeyType key) {
		if(map==null) return null;
		ValueType value = map.remove(key);
		if(map.isEmpty()) map = null;
		return value;
	}
	/**
	 * @param key
	 * @return True if key present.
	 */
	public synchronized boolean containsKey(KeyType key) {
		if(map==null) return false;
		return map.containsKey(key);
	}
	/**
	 * @param value
	 * @return True if value present.
	 */
	public synchronized boolean containsValue(ValueType value) {
		if(map==null) return false;
		return map.containsValue(value);
	}
	/**
	 * @return The key set.
	 */
	public synchronized Set<KeyType> keySet() {
		if(map==null) return new TreeSet<KeyType>();
		return map.keySet();
	}
	/**
	 * @return The values.
	 */
	public synchronized Collection<ValueType> values() {
		if(map==null) return new TreeSet<ValueType>();
		return map.values();
	}
	/**
	 * Make a copy and return. Caller can modify returned map without affecting this map.
	 * @return The copied map.
	 */
	public synchronized HashMap<KeyType,ValueType> getMap() {
		if(map==null) return new HashMap<KeyType,ValueType>(NullIfEmptyMap.CAPACITY);
		HashMap<KeyType,ValueType> copy = new HashMap<KeyType,ValueType>(map);
		return copy;
	}
	/**
	 * @return The entry set.
	 */
	public synchronized Set<Map.Entry<KeyType, ValueType>> entrySet() {
		if(map==null) return new TreeSet<Map.Entry<KeyType, ValueType>>();
		return map.entrySet(); 
	}
	/**
	 * @return The size.
	 */
	public synchronized int size() {
		if(map==null) return 0;
		return map.size();
	}
	/**
	 * @return True if empty.
	 */
	public synchronized boolean isEmpty() {
		if(map==null) return true;
		return map.isEmpty();
	}
	/**
	 * 
	 */
	public synchronized void clear() {
		if(map==null) return;
		map.clear();
	}


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}

}
