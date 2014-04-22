package edu.umass.cs.gns.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

/**
@author V. Arun
 */

/* This class implements a specific kind of a trie. The
 * key is a string concatenated with a version number.
 * In addition to get(key) that tries to retrieve an exact
 * match, it supports a match(keyPrefix) method that matches
 * the keyPrefix against the highest versioned key with
 * keyPrefix as a prefix. 'keyPrefix' must have a toString()
 * method but doesn't have to be a String. 'key' must be a 
 * String and is computed as keyPrefix.toString()+version. 
 */
public class StringVersionedMap<KeyType,ValueType> {

	private static final String SEPARATOR = ":";
	private HashMap<String,ValueType> map=null;
	private HashMap<KeyType,ArrayList<Integer>> keyVersions=null;
	
	public StringVersionedMap() {
		map = new HashMap<String,ValueType>();
		keyVersions = new HashMap<KeyType,ArrayList<Integer>>();
	}
	
	// Inserts [key = keyPrefix.toString()+version, value]
	public synchronized void put(KeyType keyPrefix, int version, ValueType value) {
		this.map.put(this.combineIDVersion(keyPrefix.toString(),version),value);
		ArrayList<Integer> versions = this.keyVersions.get(keyPrefix);
		if(versions==null) versions = new ArrayList<Integer>();
		if(!versions.contains(version)) versions.add(version);
		this.keyVersions.put(keyPrefix, versions);
	}
	/* Returns value corresponding to the highest version such 
	 * that [keyprefix.toString()+version, value] is in the map.
	 * If none is found, it will try to find an exact match for 
	 * some [keyPrefix.toString(), value] entry.
	 */
	public synchronized ValueType match(KeyType keyPrefix) {
		ValueType value = null;
		ArrayList<Integer> versions = this.keyVersions.get(keyPrefix);
		if(versions!=null) {
			int maxVersion = Integer.MIN_VALUE;
			for(int version : versions) {
				if(version > maxVersion) maxVersion = version;
			}
			value = this.map.get(StringVersionedMap.combineIDVersion(keyPrefix.toString(), maxVersion));
		}
		if(value==null) value = this.map.get(keyPrefix.toString()); // Not sure if we should do this or not
		return value;
	}
	public static String combineIDVersion(String id, int version) {
		return id + StringVersionedMap.SEPARATOR + version;
	}
	public static String getIDNoVersion(String paxosID) {
		if(paxosID==null) return null;
		String[] pieces = paxosID.split(":");
		assert(pieces!=null && pieces.length==2);
		return pieces[0];
	}
	public static int getVersion(String paxosID) {
		if(paxosID==null) return 0;
		String[] pieces = paxosID.split(":");
		assert(pieces!=null && pieces.length==2);
		return Integer.parseInt(pieces[1]);
	}
	/* Usual get with a String key. Called must know key or must
	 * use the combineIDVersion method above.
	 */
	public synchronized ValueType get(String key) {
		return this.map.get(key);
	}
	
	/************ Standard map methods below ***********************/
	public synchronized Collection<String> keySet() {
		return this.map.keySet();
	}
	public synchronized Collection<KeyType> keyPrefixSet() {
		return this.keyVersions.keySet();
	}
	public synchronized Collection<ValueType> values() {
		return this.map.values();
	}
	public synchronized boolean containsKey(String key) {
		return this.map.containsKey(key);
	}
	public synchronized boolean containsValue(ValueType value) {
		return this.map.containsValue(value);
	}
	public synchronized int size() {
		return this.map.size();
	}
	public synchronized int numKeyPrefixes() {
		return this.keyVersions.size();
	}
	public synchronized boolean isEmpty() {
		return this.map.isEmpty();
	}
	public synchronized void clear() {
		this.keyVersions.clear();
		this.map.clear();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		StringVersionedMap<String,Integer> svmap = new StringVersionedMap<String,Integer>();
		String key1 = "key1";
		int value1 = 24;
		svmap.put(key1, 0, value1);
		assert(svmap.match(key1) == value1) : "Inserted but could not match " + value1;
		assert(svmap.get("unknownkey")==null) : "Retrieved non-null value for key that was never inserted";
		String key2 = "key2";
		int value2 = 32;
		svmap.put(key2, 1, value2);
		assert(svmap.match(key2)==value2) : "Inserted but could not match <" + key2 + ","+value2+">";
		assert(svmap.get(StringVersionedMap.combineIDVersion(key2,1))==value2) : 
			"Inserted but could not get <" + (key2+1) + ","+value2+">";
		
		int value3 = 43;
		svmap.put(key1, 1, value2);
		assert(svmap.match(key1)==Math.max(value1, value2)) : "Inserted but could not match <" + key1 + ","+value2+">";
		assert(svmap.get(StringVersionedMap.combineIDVersion(key1,1))==value2) : 
			"Inserted but could not get <" + (key1+1) + ","+value2+">";
		svmap.put(key1, 2, value3);
		assert(svmap.match(key1)==Math.max(value1, Math.max(value2,value3))) : 
			"Inserted but could not match <" + key1 + ","+value3+">";
		assert(svmap.get(StringVersionedMap.combineIDVersion(key1,2))==value3) : 
			"Inserted but could not get <" + (key1+2) + ","+value3+">";
		

		/* Space testing */
		int million = 1000000;
		int size = (int)(22*million);
		//StringVersionedMap<String,String>[] svmarray = new StringVersionedMap[size];
		//HashMap<String,String>[] hmaparray = new HashMap[size];
		byte[][] bytearray = new byte[size][20];
		int j=1;
		for(int i=0; i<size; i++) {
			//svmarray[i] = new StringVersionedMap<String,String>();
			//svmarray[i].put("paxos"+i, 0, (""+i));
			//hmaparray[i] = new HashMap<String, String>();
			//hmaparray[i].put("key"+i, "value"+i);
			//strarray[i] = new String("str"+i);
			bytearray[i] = new String("str"+i).getBytes();
			if(i%j==0 || i%(million/10)==0) {System.out.println(i + " "); if(j<million/2) j*=2;}
		}
		System.out.println("Created " + size + " instances");

		System.out.println("SUCCESS");

	}

}
