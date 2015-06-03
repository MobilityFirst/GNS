package edu.umass.cs.reconfiguration.reconfigurationutils;

import java.util.HashMap;

public class StringLocker {
	private final HashMap<String,String> map = new HashMap<String,String>();
	
	public String get(String key) {
		if(map.containsKey(key)) return map.get(key);
		String newKey = new String(key);
		map.put(newKey, newKey);
		return newKey;
	}
}
