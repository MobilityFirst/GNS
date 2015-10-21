package edu.umass.cs.utils;

/**
 * @author arun
 *
 */
public interface GCConcurrentHashMapCallback {
	/**
	 * @param key
	 * @param value
	 */
	public void callbackGC(Object key, Object value);
}
