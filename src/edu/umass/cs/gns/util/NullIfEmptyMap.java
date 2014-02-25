package edu.umass.cs.gns.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;

/**
@author V. Arun
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
	
	public synchronized void put(KeyType key, ValueType value){
		if(map==null) map = new HashMap<KeyType,ValueType>(NullIfEmptyMap.CAPACITY);
		map.put(key, value);
	}
	public synchronized ValueType get(KeyType key) {
		if(map==null) return null;
		return map.get(key);
	}
	public synchronized ValueType remove(KeyType key) {
		if(map==null) return null;
		ValueType value = map.remove(key);
		if(map.isEmpty()) map = null;
		return value;
	}
	public synchronized boolean containsKey(KeyType key) {
		if(map==null) return false;
		return map.containsKey(key);
	}
	public synchronized boolean containsValue(ValueType value) {
		if(map==null) return false;
		return map.containsValue(value);
	}
	public synchronized Set<KeyType> keySet() {
		if(map==null) return new TreeSet<KeyType>();
		return map.keySet();
	}
	public synchronized Collection<ValueType> values() {
		if(map==null) return new TreeSet<ValueType>();
		return map.values();
	}
	public synchronized HashMap<KeyType,ValueType> getMap() {
		// Make a copy and return, otherwise caller can modify map.
		if(map==null) return new HashMap<KeyType,ValueType>(NullIfEmptyMap.CAPACITY);
		HashMap<KeyType,ValueType> copy = new HashMap<KeyType,ValueType>(map);
		return copy;
	}
	public synchronized int size() {
		if(map==null) return 0;
		return map.size();
	}
	public synchronized boolean isEmpty() {
		if(map==null) return true;
		return map.isEmpty();
	}
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
