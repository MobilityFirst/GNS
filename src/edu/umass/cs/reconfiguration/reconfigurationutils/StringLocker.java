package edu.umass.cs.reconfiguration.reconfigurationutils;

import java.util.HashMap;

/**
 * @author V. Arun
 * 
 *         This class is a hack in order to have the syntactic luxury of saying
 *         synchronized(key), for an arbitrary String key, and get
 *         synchronization with respect to anyone else competing also for a
 *         synchronized(key) code block. Normally, we don't get this property,
 *         i.e., synchronization on key1 and key2 are unrelated even if
 *         key1.equals(key2). With this class, the caller can simply say
 *         synchronized(stringLocker.get(key)) { ... } and ensure
 *         synchronization for the following code block with respect to anyone
 *         else also using the same stringLocker object.
 * 
 *         We need to be careful to rely on this only when we know that the
 *         total number of Strings used for synchronization will be small. It is
 *         best to use this class only for constant strings.
 */
public class StringLocker {
	private final HashMap<String, String> map = new HashMap<String, String>();

	/**
	 * @param key
	 * @return The canonical String that equals {@code key}.
	 */
	public String get(String key) {
		if (map.containsKey(key))
			return map.get(key);
		String newKey = new String(key);
		map.put(newKey, newKey);
		return newKey;
	}
	
	/**
	 * Removes key when no one is using it for synchronization. We know that no
	 * one is using it because we are using it ourselves through the
	 * {@code synchronized(get(key)} block below.
	 * 
	 * @param key
	 */
	public void remove(String key) {
		synchronized(get(key)) {
			remove(key);
		}
	}
}
